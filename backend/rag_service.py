import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List
from backend.embedding_service import embedding_service
from backend.data_tools import DataTools
from cerebras.cloud.sdk import Cerebras

from dotenv import load_dotenv

load_dotenv()

class RAGService:
    def __init__(self):
        api_key = os.getenv("CEREBRAS_API")
        self.client = Cerebras(api_key=api_key)
        self.model = "gpt-oss-120b"  # hoặc "gpt-oss-120b"
        self.conversation_history = []

    def parse_intent(self, user_message: str) -> Dict[str, Any]:
        """Parse intent nhanh bằng regex (không cần LLM)"""
        message_lower = user_message.lower()
        now = datetime.now()
        
        intent = {
            'type': 'general',
            'ticker': 'BTCUSDT',
            'time_range': None,
            'exact_time': None,
            'requires_structured_data': False,
            'requires_vector_search': True
        }
        
        # 1. Extract Ticker
        ticker_match = re.search(
            r'\b(BTC|ETH|BNB|SOL|ADA|DOT|MATIC|LINK)(?:USDT)?\b', 
            message_lower, 
            re.IGNORECASE
        )
        if ticker_match:
            ticker = ticker_match.group(1).upper()
            intent['ticker'] = f"{ticker}USDT" if not ticker.endswith('USDT') else ticker
        
        # 2. Detect Query Type
        if any(kw in message_lower for kw in ['giá', 'price', 'bao nhiêu', 'how much']):
            intent['type'] = 'price_query'
            intent['requires_structured_data'] = True
            
        elif any(kw in message_lower for kw in ['xu hướng', 'trend', 'phân tích', 'analyze', 'biến động']):
            intent['type'] = 'trend_analysis'
            intent['requires_structured_data'] = True
            intent['requires_vector_search'] = True
            
        elif any(kw in message_lower for kw in ['tin tức', 'news', 'sentiment', 'tâm lý']):
            intent['type'] = 'news_summary'
            intent['requires_vector_search'] = True
        
        # 3. Parse Time
        
        # 3a. Exact date: YYYY-MM-DD, DD/MM/YYYY
        date_patterns = [
            (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
            (r'(\d{2})/(\d{2})/(\d{4})', '%d/%m/%Y'),
        ]
        
        for pattern, fmt in date_patterns:
            match = re.search(pattern, user_message)
            if match:
                try:
                    date_obj = datetime.strptime(match.group(0), fmt)
                    intent['exact_time'] = date_obj.strftime('%Y-%m-%d')
                    return intent
                except:
                    pass
        
        # 3b. "X ngày gần nhất/qua/trước"
        days_pattern = r'(\d+)\s*ngày\s*(gần nhất|qua|trước|gần đây)'
        match = re.search(days_pattern, message_lower)
        if match:
            days_count = int(match.group(1))
            end_date = now
            start_date = now - timedelta(days=days_count)
            intent['time_range'] = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
            intent['requires_structured_data'] = True
            return intent
        
        # 3c. Relative time keywords
        relative_time_map = {
            'hôm nay': 0,
            'today': 0,
            'hôm qua': -1,
            'yesterday': -1,
            'hôm kia': -2,
        }
        
        for keyword, offset in relative_time_map.items():
            if keyword in message_lower:
                target_date = now + timedelta(days=offset)
                intent['exact_time'] = target_date.strftime('%Y-%m-%d')
                return intent
        
        # 3d. Week/month ranges
        if 'tuần này' in message_lower or 'this week' in message_lower:
            intent['time_range'] = {
                'start': (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d'),
                'end': now.strftime('%Y-%m-%d')
            }
        elif 'tuần trước' in message_lower or 'last week' in message_lower:
            start = now - timedelta(days=now.weekday() + 7)
            end = start + timedelta(days=6)
            intent['time_range'] = {
                'start': start.strftime('%Y-%m-%d'),
                'end': end.strftime('%Y-%m-%d')
            }
        elif 'tháng này' in message_lower or 'this month' in message_lower:
            intent['time_range'] = {
                'start': now.replace(day=1).strftime('%Y-%m-%d'),
                'end': now.strftime('%Y-%m-%d')
            }
        elif 'tháng trước' in message_lower or 'last month' in message_lower:
            last_month = now.replace(day=1) - timedelta(days=1)
            intent['time_range'] = {
                'start': last_month.replace(day=1).strftime('%Y-%m-%d'),
                'end': last_month.strftime('%Y-%m-%d')
            }
        
        # Default for trend analysis: 7 days
        if intent['type'] == 'trend_analysis' and not intent['time_range'] and not intent['exact_time']:
            intent['time_range'] = {
                'start': (now - timedelta(days=7)).strftime('%Y-%m-%d'),
                'end': now.strftime('%Y-%m-%d')
            }
        
        return intent

    def create_context_from_results(self, results: List[Dict[str, Any]]) -> str:
        """Tạo context từ vector search"""
        if not results:
            return "Không tìm thấy thông tin liên quan."
        
        context_parts = []
        for idx, r in enumerate(results, 1):
            content = r.get('document', '')
            meta = r.get('metadata', {})
            date_info = f"[{meta.get('date', 'Unknown')}]" if 'date' in meta else ""
            context_parts.append(f"{idx}. {date_info} {content[:300]}...")
            
        return "\n\n".join(context_parts)
    
    def chat(
        self,
        user_message: str,
        search_collections: List[str] = None,
        top_k: int = 5,
        temperature: float = 0.3,
        max_tokens: int = 4096
    ) -> Dict[str, Any]:
        
        # 1. Parse Intent
        intent = self.parse_intent(user_message)
        ticker = intent['ticker']
        # # Inherit từ session nếu user không nói rõ
        # if not intent['ticker'] and self.session_context['current_ticker']:
        #     intent['ticker'] = self.session_context['current_ticker']

        # # Update session
        # self.session_context.update({
        #     'current_ticker': intent['ticker'],
        #     'current_timeframe': intent.get('time_range'),
        #     'last_intent': intent
        # })
        
        # 2. Get Structured Data (SQL)
        structured_data = ""
        
        if intent['requires_structured_data']:
            result = None
            if intent['exact_time']:
                if ' ' in intent['exact_time'] and ':' in intent['exact_time']:
                    result = DataTools.get_price_at_moment(ticker, intent['exact_time'])
                else:
                    result = DataTools.get_exact_price(ticker, intent['exact_time'])
            elif intent['time_range']:
                result = DataTools.get_price_range(
                    ticker, 
                    intent['time_range']['start'], 
                    intent['time_range']['end']
                )
            else:
                result = DataTools.get_market_overview(ticker)
            
            # ← XỬ LÝ ERROR
            if isinstance(result, dict):
                if result['status'] == 'no_data':
                    structured_data = f"""
          **KHÔNG CÓ DỮ LIỆU TRONG DATABASE**

        {result['message']}
         Gợi ý: {result['suggestion']}
        """
                elif result['status'] == 'error':
                    structured_data = f"❌ Lỗi: {result['message']}"
                else:
                    structured_data = result.get('formatted', str(result))
            else:
                structured_data = result

        # 3. Get Unstructured Data (Vector Search)
        vector_context = ""
        search_results = []
        
        if intent['requires_vector_search']:
            search_results = embedding_service.search(
                query=user_message,
                collection_types=search_collections or ['news', 'analysis'],
                top_k=top_k,
                time_range=intent.get('time_range')
            )
            vector_context = self.create_context_from_results(search_results)

        # 4. Build Context
        full_context = f"""
                    {'='*60}
                    STRUCTURED DATA (Facts from Database - Priority #1):
                    {'='*60}
                    {structured_data if structured_data else 'Không có dữ liệu số học cụ thể.'}

                    {'='*60}
                    UNSTRUCTURED DATA (Context & Analysis - Priority #2):
                    {'='*60}
                    {vector_context if vector_context else 'Không có phân tích/tin tức liên quan.'}
                    """

        # 5. System Prompt
        system_prompt = f"""
        Bạn là CryptoAI Assistant - Chuyên gia phân tích tiền điện tử.
        **Ngày giờ hiện tại:** {datetime.now().strftime('%d/%m/%Y %H:%M')}
        
        NHIỆM VỤ:
        1. **Trả lời chính xác** các câu hỏi về giá/volume dựa trên STRUCTURED DATA
        2. **Phân tích xu hướng** dựa trên UNSTRUCTURED DATA (tin tức, sentiment)
        3. **Kết hợp 2 nguồn** để đưa ra nhận định toàn diện
        4. **Trung thực:** Nếu không có dữ liệu, nói rõ "Không có dữ liệu"

        QUY TẮC HIỂN THỊ (BẮT BUỘC):
        1. Với công thức toán học inline (cùng dòng), dùng dấu $: ví dụ $x^2$.
        2. Với công thức toán học block (riêng dòng), dùng dấu $$: ví dụ $$ \sum i $$.
        3. KHÔNG ĐƯỢC DÙNG các định dạng như \( \) hay \[ \].
        4. Trình bày bảng biểu, in đậm rõ ràng.
        5. Bold cho con số quan trọng

        **INTENT DETECTED:** {intent['type']}
        """

        # 6. Call LLM
        messages = [
            {"role": "system", "content": system_prompt}
        ]
        
        # Add conversation history (CHỈ câu gốc, KHÔNG có context)
        messages.extend(self.conversation_history[-10:])  # Last 10 exchanges
        
        # Current message WITH context (CHỈ message hiện tại)
        current_message = f"{full_context}\n\n**Câu hỏi:** {user_message}"
        messages.append({"role": "user", "content": current_message})
        
        # 7. Call LLM
        try:
            completion = self.client.chat.completions.create(
                messages=messages,
                model=self.model,
                max_completion_tokens=max_tokens,
                temperature=temperature,
                stream=False
            )
            
            response_text = completion.choices[0].message.content
            
            # 8. Save to history (GỐC không có context)
            self.conversation_history.append({
                "role": "user", 
                "content": user_message  # ← Lưu câu gốc
            })
            self.conversation_history.append({
                "role": "assistant", 
                "content": response_text
            })
            
            return {
                "response": response_text,
                "sources": search_results[:3] if search_results else [],
                "intent": intent,
                "has_structured_data": bool(structured_data),
                "has_vector_data": bool(vector_context)
            }
            
        except Exception as e:
            print(f"LLM Error: {e}")
            return {
                "response": f"❌ Xin lỗi, tôi gặp lỗi: {str(e)}",
                "error": True
            }

    def analyze_price_trend(self, ticker, timeframe="recent", technical_indicators=None):
        return self.chat(f"Analyze price trend for {ticker} ({timeframe})")['response']

    def analyze_news_sentiment(self, ticker, days=7):
        return self.chat(f"Analyze news sentiment for {ticker} last {days} days")['response']

    def clear_history(self):
        self.conversation_history = []
        print("✓ Conversation history cleared")


# Singleton
rag_service = RAGService()