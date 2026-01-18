import React, { useState, useRef, useEffect } from "react";
import { Send, Bot, User, Loader, X, Trash2, Search } from "lucide-react";
import { chatWithBot, clearChatHistory, searchKnowledgeBase } from "../services/api";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';   
import rehypeKatex from 'rehype-katex';  
import 'katex/dist/katex.min.css';

const Chatbot = () => {
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content: "Hello! I'm your crypto trading assistant. I can help you analyze price trends, understand technical indicators, and interpret market news. What would you like to know?",
      timestamp: new Date()
    }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [showSources, setShowSources] = useState({});
  const messagesEndRef = useRef(null);
  const chatContainerRef = useRef(null);

  const scrollToBottom = () => {
    if (chatContainerRef.current) {
      const { scrollHeight, clientHeight } = chatContainerRef.current;
      // Thiết lập vị trí cuộn xuống đáy của container
      chatContainerRef.current.scrollTo({
        top: scrollHeight - clientHeight,
        behavior: "smooth"
      });
    }
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || loading) return;
    const userMessage = input.trim();
    setInput("");

    setMessages(prev => [...prev, { role: "user", content: userMessage, timestamp: new Date() }]);
    setLoading(true);

    try {
      const response = await chatWithBot({
        message: userMessage,
        top_k: 5,
        temperature: 0.3
      });

      setMessages(prev => [...prev, {
        role: "assistant",
        content: response.response,
        sources: response.sources,
        timestamp: new Date()
      }]);
    } catch (error) {
      setMessages(prev => [...prev, { role: "assistant", content: "Error occurred.", error: true, timestamp: new Date() }]);
    } finally {
      setLoading(false);
    }
  };

  const handleClear = async () => {
    try {
      await clearChatHistory();
      setMessages([{
        role: "assistant",
        content: "Chat history cleared. How can I help you?",
        timestamp: new Date()
      }]);
    } catch (error) {
      console.error("Error clearing history:", error);
    }
  };

  const toggleSources = (index) => {
    setShowSources(prev => ({
      ...prev,
      [index]: !prev[index]
    }));
  };

  const quickQuestions = [
    "What's the current trend for BTC?",
    "Explain RSI indicator",
    "Latest crypto news sentiment",
    "How to interpret MACD?"
  ];

  const handleQuickQuestion = (question) => {
    setInput(question);
  };

  const preprocessLaTeX = (content) => {
    if (!content) return "";
    
    // 1. Thay thế block math \[ ... \] thành $$ ... $$
    const blockReplaced = content.replace(/\\\[/g, '$$').replace(/\\\]/g, '$$');
    
    // 2. Thay thế inline math \( ... \) thành $ ... $
    // Lưu ý: Regex này cần cẩn thận để không thay thế nhầm các dấu ngoặc khác
    const inlineReplaced = blockReplaced.replace(/\\\(/g, '$').replace(/\\\)/g, '$');
    
    return inlineReplaced;
  };

  return (
    <div className="flex-1 h-full flex flex-col bg-slate-900 overflow-hidden">
      {/* Header */}
      <div className="flex-shrink-0 flex items-center justify-between p-4 bg-slate-800 border-b border-slate-700">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-600 rounded-lg">
            <Bot size={24} className="text-white" />
          </div>
          <div>
            <h2 className="text-lg font-bold text-white">Crypto AI Assistant</h2>
            {/* <p className="text-xs text-slate-400">Powered by RAG & Cerebras</p> */}
          </div>
        </div>
        <button
          onClick={handleClear}
          className="p-2 hover:bg-slate-700 rounded-lg transition-colors text-slate-400 hover:text-white"
          title="Clear chat history"
        >
          <Trash2 size={20} />
        </button>
      </div>

      {/* Quick Questions */}
      {messages.length === 1 && (
        <div className="flex-shrink-0 p-4 bg-slate-800/50 border-t border-slate-700">
          <p className="text-xs text-slate-400 mb-2">Quick questions:</p>
          <div className="flex flex-wrap gap-2">
            {quickQuestions.map((q, idx) => (
              <button
                key={idx}
                onClick={() => handleQuickQuestion(q)}
                className="px-3 py-1.5 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg text-xs transition-colors"
              >
                {q}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Messages */}
      <div ref={chatContainerRef} className="flex-1 min-h-0 overflow-y-auto p-4 space-y-4 custom-scrollbar">
        {messages.map((msg, idx) => (
          <div
            key={idx}
            className={`flex gap-3 ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            {msg.role === "assistant" && (
              <div className="flex-shrink-0 w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
                <Bot size={18} className="text-white" />
              </div>
            )}

            <div className={`flex flex-col gap-2 max-w-[80%] ${msg.role === "user" ? "items-end" : "items-start"}`}>
              <div
                className={`px-4 py-3 rounded-2xl ${
                  msg.role === "user"
                    ? "bg-blue-600 text-white"
                    : msg.error
                    ? "bg-red-900/30 text-red-300 border border-red-700"
                    : "bg-slate-800 text-slate-100 border border-slate-700"
                }`}
              >
                {/* <p className="text-sm whitespace-pre-wrap">{msg.content}</p> */}
                <div className="text-sm markdown-body">
                  <ReactMarkdown 
                    remarkPlugins={[remarkGfm, remarkMath]}
                    rehypePlugins={[rehypeKatex]}
                    components={{
                      // Tùy chỉnh style cho các thẻ cụ thể nếu cần
                      p: ({node, ...props}) => <p className="mb-2 last:mb-0 leading-relaxed" {...props} />,
                      a: ({node, ...props}) => <a className="text-blue-400 hover:underline cursor-pointer" target="_blank" {...props} />,
                      ul: ({node, ...props}) => <ul className="list-disc list-inside mb-2 pl-2" {...props} />,
                      ol: ({node, ...props}) => <ol className="list-decimal list-inside mb-2 pl-2" {...props} />,
                      li: ({node, ...props}) => <li className="mb-1" {...props} />,
                      table: ({node, ...props}) => (
                        <div className="overflow-x-auto my-3 border border-slate-600 rounded-lg">
                          <table className="w-full text-left border-collapse text-sm" {...props} />
                        </div>
                      ),
                      thead: ({node, ...props}) => <thead className="bg-slate-700 text-slate-200" {...props} />,
                      th: ({node, ...props}) => <th className="p-3 border-b border-r border-slate-600 font-semibold last:border-r-0" {...props} />,
                      td: ({node, ...props}) => <td className="p-3 border-b border-r border-slate-600 last:border-r-0" {...props} />,
                      blockquote: ({node, ...props}) => <blockquote className="border-l-4 border-blue-500 pl-4 py-1 my-2 bg-slate-700/50 italic text-slate-300 rounded-r" {...props} />,
                      code: ({node, inline, className, children, ...props}) => {
                        return inline ? (
                          <code className="bg-slate-700 px-1.5 py-0.5 rounded text-yellow-300 font-mono text-xs" {...props}>{children}</code>
                        ) : (
                          <div className="my-3 rounded-lg overflow-hidden bg-slate-950 border border-slate-700">
                            <div className="px-3 py-1 bg-slate-700/50 text-xs text-slate-400 font-mono border-b border-slate-700">Code</div>
                            <code className="block p-3 overflow-x-auto text-yellow-300 font-mono text-xs" {...props}>{children}</code>
                          </div>
                        )
                      }
                    }}
                  >
                    {preprocessLaTeX(msg.content)}
                  </ReactMarkdown>
                </div>
              </div>

              {/* Sources */}
              {msg.sources && msg.sources.length > 0 && (
                <div className="w-full">
                  <button
                    onClick={() => toggleSources(idx)}
                    className="text-xs text-slate-400 hover:text-slate-300 flex items-center gap-1"
                  >
                    <Search size={12} />
                    {showSources[idx] ? "Hide" : "Show"} sources ({msg.sources.length})
                  </button>

                  {showSources[idx] && (
                    <div className="mt-2 space-y-2">
                      {msg.sources.map((source, sidx) => (
                        <div
                          key={sidx}
                          className="p-3 bg-slate-900/50 border border-slate-700 rounded-lg"
                        >
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-xs font-mono text-blue-400">
                              {source.collection}
                            </span>
                            <span className="text-xs text-slate-500">
                              {(source.relevance_score * 100).toFixed(1)}% relevant
                            </span>
                          </div>
                          <p className="text-xs text-slate-300">{source.excerpt}</p>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              <span className="text-xs text-slate-500">
                {msg.timestamp.toLocaleTimeString()}
              </span>
            </div>

            {msg.role === "user" && (
              <div className="flex-shrink-0 w-8 h-8 bg-slate-700 rounded-full flex items-center justify-center">
                <User size={18} className="text-slate-300" />
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div className="flex gap-3">
            <div className="flex-shrink-0 w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
              <Bot size={18} className="text-white" />
            </div>
            <div className="px-4 py-3 bg-slate-800 border border-slate-700 rounded-2xl">
              <div className="flex items-center gap-2 text-slate-400">
                <Loader className="animate-spin" size={16} />
                <span className="text-sm">Thinking...</span>
              </div>
            </div>
          </div>
        )}

        {/* <div ref={messagesEndRef} /> */}
      </div>

      {/* Input */}
      <div className="flex-shrink-0 p-4 bg-slate-800 border-t border-slate-700">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => e.key === "Enter" && handleSend()}
            placeholder="Ask about crypto trends, indicators, news..."
            className="flex-1 px-4 py-3 bg-slate-900 text-white border border-slate-600 rounded-lg focus:outline-none focus:border-blue-500 placeholder-slate-500"
            disabled={loading}
          />
          <button
            onClick={handleSend}
            disabled={!input.trim() || loading}
            className="px-6 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-700 disabled:cursor-not-allowed text-white rounded-lg font-medium transition-colors flex items-center gap-2"
          >
            <Send size={18} />
          </button>
        </div>
        <p className="text-xs text-slate-500 mt-2">
          Pro tip: Ask specific questions about price trends, technical indicators, or news sentiment
        </p>
      </div>
    </div>
  );
};

export default Chatbot;