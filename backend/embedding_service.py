import os
from typing import List, Dict, Any, Optional
from enum import Enum
import numpy as np
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
import json


class CandleInterval(Enum):
    """Các loại nến hỗ trợ"""
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"


class EmbeddingConfig:
    """Cấu hình cho từng loại dữ liệu"""
    def __init__(self):
        # Cấu hình cho KLINE
        self.kline_enabled = True
        self.kline_days = 30  # Giảm từ 90 xuống 30
        self.kline_interval = CandleInterval.ONE_HOUR  # Đổi từ 5m → 1h
        
        # Cấu hình cho NEWS
        self.news_enabled = True
        self.news_days = 30
        
        # Cấu hình cho OPEN INTEREST
        self.oi_enabled = True
        self.oi_days = 30  # Giảm từ 90 xuống 30
        
        # Cấu hình cho ANALYSIS GUIDES
        self.analysis_enabled = True
    
    def to_dict(self):
        """Export config as dict"""
        return {
            "kline": {
                "enabled": self.kline_enabled,
                "days": self.kline_days,
                "interval": self.kline_interval.value
            },
            "news": {
                "enabled": self.news_enabled,
                "days": self.news_days
            },
            "open_interest": {
                "enabled": self.oi_enabled,
                "days": self.oi_days
            },
            "analysis": {
                "enabled": self.analysis_enabled
            }
        }


class EmbeddingService:
    """Service for creating and managing embeddings"""
    
    def __init__(
        self, 
        model_name: str = "intfloat/e5-base-v2", 
        persist_dir: str = "./chroma_db",
        config: Optional[EmbeddingConfig] = None
    ):
        print(f"Initializing embedding service with model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.persist_dir = persist_dir
        self.config = config or EmbeddingConfig()
        
        # Initialize ChromaDB client
        self.client = chromadb.PersistentClient(
            path=persist_dir,
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )
        
        # Create collections for different data types
        self.collections = {}
        if self.config.kline_enabled:
            self.collections['kline'] = self._get_or_create_collection('kline_data')
        if self.config.news_enabled:
            self.collections['news'] = self._get_or_create_collection('news_data')
        if self.config.oi_enabled:
            self.collections['open_interest'] = self._get_or_create_collection('open_interest_data')
        if self.config.analysis_enabled:
            self.collections['analysis'] = self._get_or_create_collection('analysis_data')
        
        print(f"Active collections: {list(self.collections.keys())}")
        
    def _get_or_create_collection(self, name: str):
        """Get or create a ChromaDB collection"""
        try:
            return self.client.get_collection(name=name)
        except Exception:
            return self.client.create_collection(
                name=name,
                metadata={"hnsw:space": "cosine"}
            )
    
    def add_e5_prefix(self, text: str, is_query: bool = False) -> str:
        """Add E5 model prefix for better performance"""
        if is_query:
            return f"query: {text}"
        return f"passage: {text}"
    
    def embed_text(self, texts: List[str], is_query: bool = False) -> np.ndarray:
        """Generate embeddings for texts"""
        prefixed_texts = [self.add_e5_prefix(t, is_query) for t in texts]
        embeddings = self.model.encode(prefixed_texts, normalize_embeddings=True)
        return embeddings
    
    def add_kline_data(self, data: List[Dict[str, Any]], resample_to: str = "1h"):
        """
        Add OHLCV data to vector store với resampling
        
        Args:
            data: List of OHLCV records
            resample_to: '1h', '4h', '1d' (default: '1h')
        """
        if 'kline' not in self.collections:
            print("Kline collection disabled, skipping...")
            return
        
        import pandas as pd
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        if 'openTime' in df.columns:
            df['datetime'] = pd.to_datetime(df['openTime'], unit='ms')
            df = df.set_index('datetime')
        
        # Resample nếu cần
        if resample_to and resample_to != '5m':
            df_resampled = df.resample(resample_to).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            print(f"Resampled from {len(df)} to {len(df_resampled)} candles ({resample_to})")
            df = df_resampled
        
        documents = []
        metadatas = []
        ids = []
        
        for idx, (timestamp, row) in enumerate(df.iterrows()):
            # Create descriptive text for embedding
            text = f"""
            Ticker: {data[0].get('ticker', 'Unknown')}
            Time: {timestamp.strftime('%Y-%m-%d %H:%M')}
            Interval: {resample_to}
            Open: {row['open']:.2f}
            High: {row['high']:.2f}
            Low: {row['low']:.2f}
            Close: {row['close']:.2f}
            Volume: {row['volume']:.2f}
            """
            
            documents.append(text.strip())
            metadatas.append({
                'type': 'kline',
                'ticker': str(data[0].get('ticker', '')),
                'timestamp': str(int(timestamp.timestamp())),
                'interval': resample_to,
                'date': timestamp.strftime('%Y-%m-%d')
            })
            ids.append(f"kline_{data[0].get('ticker', '')}_{int(timestamp.timestamp())}_{resample_to}")
        
        if documents:
            embeddings = self.embed_text(documents, is_query=False)
            self.collections['kline'].add(
                embeddings=embeddings.tolist(),
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            print(f"✓ Added {len(documents)} kline records ({resample_to})")
    
    def add_news_data(self, data: List[Dict[str, Any]]):
        """Add news data to vector store"""
        if 'news' not in self.collections:
            print("News collection disabled, skipping...")
            return
        
        documents = []
        metadatas = []
        ids = []
        
        for idx, item in enumerate(data):
            text = f"""
            Title: {item.get('title', 'No title')}
            Subtitle: {item.get('subtitle', '')}
            Published: {item.get('publishedOn', 'Unknown')}
            Source: {item.get('sourceName', 'Unknown')}
            Sentiment: {item.get('sentiment', 'NEUTRAL')}
            Categories: {item.get('categories', '')}
            Keywords: {item.get('keywords', '')}
            Body: {item.get('rawBody', '')[:500]}
            """
            
            documents.append(text.strip())
            metadatas.append({
                'type': 'news',
                'title': str(item.get('title', '')),
                'sentiment': str(item.get('sentiment', '')),
                'source': str(item.get('sourceName', '')),
                'timestamp': str(item.get('publishedOn', '')),
                'url': str(item.get('url', '')),
            })
            ids.append(f"news_{item.get('id', idx)}_{idx}")
        
        if documents:
            embeddings = self.embed_text(documents, is_query=False)
            self.collections['news'].add(
                embeddings=embeddings.tolist(),
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            print(f"✓ Added {len(documents)} news records")
    
    def add_open_interest_data(self, data: List[Dict[str, Any]]):
        """Add open interest data to vector store"""
        if 'open_interest' not in self.collections:
            print("Open interest collection disabled, skipping...")
            return
        
        documents = []
        metadatas = []
        ids = []
        
        for idx, item in enumerate(data):
            text = f"""
            Symbol: {item.get('symbol', 'Unknown')}
            Time: {item.get('timestamp', 'Unknown')}
            Open Interest: {item.get('sumOpenInterest', 0):.2f}
            Open Interest Value: {item.get('sumOpenInterestValue', 0):.2f}
            Circulating Supply: {item.get('CMCCirculatingSupply', 0):.2f}
            """
            
            documents.append(text.strip())
            metadatas.append({
                'type': 'open_interest',
                'symbol': str(item.get('symbol', '')),
                'timestamp': str(item.get('timestamp', '')),
            })
            ids.append(f"oi_{item.get('symbol', '')}_{item.get('timestamp', '')}_{idx}")
        
        if documents:
            embeddings = self.embed_text(documents, is_query=False)
            self.collections['open_interest'].add(
                embeddings=embeddings.tolist(),
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            print(f"✓ Added {len(documents)} open interest records")
    
    def add_analysis_result(self, analysis: str, metadata: Dict[str, Any], doc_id: str):
        """Add technical analysis result to vector store"""
        if 'analysis' not in self.collections:
            print("Analysis collection disabled, skipping...")
            return
        
        embeddings = self.embed_text([analysis], is_query=False)
        
        self.collections['analysis'].add(
            embeddings=embeddings.tolist(),
            documents=[analysis],
            metadatas=[{
                'type': 'analysis',
                **metadata
            }],
            ids=[doc_id]
        )
        print(f"✓ Added analysis: {doc_id}")
    
    def search(
        self, 
        query: str, 
        collection_types: List[str] = None, 
        top_k: int = 5,
        time_range: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search across collections with optional time filtering
        
        Args:
            query: Search query
            collection_types: Which collections to search
            top_k: Number of results
            time_range: {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
        """
        if collection_types is None:
            collection_types = list(self.collections.keys())
        
        query_embedding = self.embed_text([query], is_query=True)[0]
        all_results = []
        
        for col_type in collection_types:
            if col_type not in self.collections:
                continue
            
            try:
                # Build where filter
                where_filter = None
                if time_range and col_type in ['kline', 'news', 'open_interest']:
                    import pandas as pd
                    start_ts = str(int(pd.to_datetime(time_range['start']).timestamp()))
                    end_ts = str(int(pd.to_datetime(time_range['end']).timestamp()))
                    
                    where_filter = {
                        "$and": [
                            {"timestamp": {"$gte": start_ts}},
                            {"timestamp": {"$lte": end_ts}}
                        ]
                    }
                
                results = self.collections[col_type].query(
                    query_embeddings=[query_embedding.tolist()],
                    n_results=top_k,
                    where=where_filter
                )
                
                for i in range(len(results['ids'][0])):
                    all_results.append({
                        'id': results['ids'][0][i],
                        'document': results['documents'][0][i],
                        'metadata': results['metadatas'][0][i],
                        'distance': results['distances'][0][i],
                        'collection': col_type
                    })
            except Exception as e:
                print(f"Error searching {col_type}: {e}")
                continue
        
        all_results.sort(key=lambda x: x['distance'])

        top_results = all_results[:top_k * 2]  # Lấy nhiều hơn để rerank
        reranked = self.rerank_results(top_results, query, time_range)

        return all_results[:top_k]
    
    def rerank_results(
        self, 
        results: List[Dict[str, Any]], 
        query: str,
        time_range: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Rerank search results với multiple signals
        """
        import pandas as pd
        
        query_lower = query.lower()
        query_tokens = set(query_lower.split())
        
        for r in results:
            base_score = 1 - r['distance']  # Cosine similarity
            
            # 1. Keyword matching boost
            doc_lower = r['document'].lower()
            doc_tokens = set(doc_lower.split())
            keyword_overlap = len(query_tokens & doc_tokens) / max(len(query_tokens), 1)
            keyword_boost = 1 + (keyword_overlap * 0.3)
            
            # 2. Title/important field boost
            if 'title' in r.get('metadata', {}).get('type', ''):
                keyword_boost *= 1.2
            
            # 3. Recency boost (nếu có time_range)
            recency_boost = 1.0
            if time_range and 'timestamp' in r.get('metadata', {}):
                try:
                    doc_ts = int(r['metadata']['timestamp'])
                    target_end = int(pd.to_datetime(time_range['end']).timestamp())
                    
                    # Càng gần end date càng cao điểm
                    days_diff = abs(doc_ts - target_end) / 86400
                    if days_diff < 1:
                        recency_boost = 1.3
                    elif days_diff < 7:
                        recency_boost = 1.15
                    elif days_diff < 30:
                        recency_boost = 1.05
                except:
                    pass
            
            # 4. Collection type boost
            collection_boost = 1.0
            if 'news' in query_lower and r['collection'] == 'news':
                collection_boost = 1.2
            elif 'giá' in query_lower or 'price' in query_lower:
                if r['collection'] == 'kline':
                    collection_boost = 1.25
            
            # Final score
            r['final_score'] = base_score * keyword_boost * recency_boost * collection_boost
            r['boosts'] = {
                'base': base_score,
                'keyword': keyword_boost,
                'recency': recency_boost,
                'collection': collection_boost
            }
        
        # Sort by final score
        return sorted(results, key=lambda x: x['final_score'], reverse=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about collections"""
        stats = {}
        for name, collection in self.collections.items():
            try:
                count = collection.count()
                stats[name] = {"count": count, "enabled": True}
            except:
                stats[name] = {"count": 0, "enabled": False}
        return stats
    
    def reset_collection(self, collection_name: str):
        """Reset a specific collection"""
        if collection_name in self.collections:
            self.client.delete_collection(name=f"{collection_name}_data")
            self.collections[collection_name] = self._get_or_create_collection(f"{collection_name}_data")
            print(f"✓ Reset collection: {collection_name}")
    
    def reset_all(self):
        """Reset all collections"""
        for name in list(self.collections.keys()):
            self.reset_collection(name)
        print("✓ Reset all collections")


# Global config instance
default_config = EmbeddingConfig()

# Singleton instance with default config
embedding_service = EmbeddingService(config=default_config)