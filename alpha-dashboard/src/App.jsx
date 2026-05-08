import React, { useState, useEffect } from 'react';

const SignalCard = ({ ticker }) => {
  const [data, setData] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(`http://localhost:8000/snapshot/${ticker}`);
        const result = await response.json();
        setData(result);
      } catch (err) { console.error("API Down"); }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000); // Poll every 2s
    return () => clearInterval(interval);
  }, [ticker]);

  // SKELETON LOADING STATE (MUCH PRETTIER)
  if (!data) return (
    <div className="p-6 bg-neutral-900 border border-neutral-800 rounded-3xl animate-pulse space-y-4">
      <div className="flex justify-between">
        <div className="h-5 w-20 bg-neutral-700 rounded-full"></div>
        <div className="h-6 w-16 bg-neutral-700 rounded-full"></div>
      </div>
      <div className="h-14 w-full bg-neutral-700 rounded-lg"></div>
      <div className="h-4 w-1/2 bg-neutral-700 rounded"></div>
      <div className="h-10 w-full bg-neutral-700 rounded"></div>
    </div>
  );

  const isBuy = data.signal === "BUY";
  const color = isBuy ? 'emerald' : 'rose';

  return (
    // CARD CONTAINER - Sleek, Rounded, Depth
    <div className={`
      p-6 rounded-3xl bg-neutral-950 border border-neutral-800 shadow-[0_0_60px_-10px_rgba(0,0,0,0.7)]
      transition-all duration-300 ease-out hover:scale-[1.02] hover:-translate-y-1 hover:border-${color}-900
    `}>
      {/* Ticker and Signal Badge */}
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-sm font-bold tracking-[0.2em] text-neutral-500 uppercase">{data.ticker}</h2>
        <span className={`
          px-4 py-1.5 rounded-full text-xs font-extrabold tracking-widest uppercase
          bg-${color}-500/10 text-${color}-400 border border-${color}-800/30
        `}>
          {data.signal}
        </span>
      </div>
      
      {/* The Price - Big, Mono, Professional */}
      <div className={`
        text-6xl font-mono font-bold text-white tracking-tighter leading-none
        bg-gradient-to-br from-white to-neutral-400 bg-clip-text text-transparent
      `}>
        ${parseFloat(data.price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
      </div>

      {/* The Sentiment Footer */}
      <div className="mt-8 border-t border-neutral-800 pt-6 space-y-2">
        <p className="text-[10px] text-neutral-500 uppercase font-black tracking-[0.25em]">Sentiment Engine Basis</p>
        <p className="text-sm text-neutral-300 italic font-medium leading-relaxed line-clamp-2">
          “{data.headline}”
        </p>
      </div>
    </div>
  );
};

const MacroHeader = () => {
  const [vitals, setVitals] = useState({});

  useEffect(() => {
    const fetchMacros = async () => {
      try {
        const response = await fetch('http://localhost:8000/macro/vitals');
        const data = await response.json();
        setVitals(data);
      } catch (err) { console.error("Macro API error"); }
    };

    fetchMacros();
    const interval = setInterval(fetchMacros, 5000); // Macros change slowly, 5s is plenty
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="flex flex-wrap gap-4 mb-10 overflow-x-auto pb-4 scrollbar-hide">
      {Object.entries(vitals).map(([name, info]) => (
        <div key={name} className="flex flex-col px-5 py-3 bg-neutral-900/50 border border-neutral-800 rounded-2xl min-w-[180px]">
          <span className="text-[10px] text-neutral-500 font-black uppercase tracking-[0.2em] mb-1">{name}</span>
          <div className="flex items-baseline gap-2">
            <span className="text-xl font-mono font-bold text-white">{info.value}%</span>
            <span className={`text-[10px] font-bold ${info.trend === 'up' ? 'text-rose-500' : 'text-emerald-500'}`}>
              {info.trend === 'up' ? '▲' : '▼'}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
};

const NewsFeed = () => {
  const [news, setNews] = useState([]);

  useEffect(() => {
    const fetchNews = async () => {
      try {
        const response = await fetch('http://localhost:8000/news/recent');
        const data = await response.json();
        setNews(data);
      } catch (err) { console.error("News API error"); }
    };

    fetchNews();
    const interval = setInterval(fetchNews, 3000); 
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-neutral-950/50 border border-neutral-800 rounded-3xl p-6 flex flex-col h-[calc(100vh-280px)] sticky top-8">
      {/* HEADER SECTION */}
      <div className="flex items-center gap-2 mb-6 border-b border-neutral-800/50 pb-4 shrink-0">
        <div className="h-2 w-2 bg-blue-500 rounded-full animate-pulse shadow-[0_0_8px_rgba(59,130,246,0.5)]"></div>
        <h2 className="text-xs font-black tracking-[0.2em] uppercase text-neutral-500">Live Intel Feed</h2>
      </div>
      
      {/* SCROLLABLE CONTENT AREA */}
      <div className="flex-1 overflow-y-auto pr-2 space-y-6 custom-scrollbar">
        {news.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full opacity-20">
             <p className="text-xs font-mono uppercase tracking-tighter">Connecting to Reuters/Alpha Stream...</p>
          </div>
        )}
        
        {Array.isArray(news) && news.length > 0 ? news.map((item, i) => (
          <div key={i} className="group border-l border-neutral-800 hover:border-blue-500/50 pl-4 py-1 transition-all duration-300">
            <div className="flex items-center justify-between mb-2">
              <span className={`text-[9px] font-black px-2 py-0.5 rounded tracking-widest uppercase ${
                item.sentiment === 'bullish' ? 'bg-emerald-500/10 text-emerald-500' : 
                item.sentiment === 'bearish' ? 'bg-rose-500/10 text-rose-500' : 'bg-neutral-800 text-neutral-400'
              }`}>
                {item.ticker}
              </span>
              <span className="text-[9px] font-mono text-neutral-600">
                {new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </span>
            </div>
            <p className="text-sm text-neutral-400 leading-snug group-hover:text-white transition-colors cursor-default font-medium">
              {item.headline}
            </p>
          </div>
        )) : null}
      </div>
    </div>
  );
};

export default function App() {
  return (
    <div className="min-h-screen bg-[#050505] p-6 lg:p-12 font-sans text-neutral-100">
      {/* HEADER */}
      <header className="flex items-center justify-between mb-10 border-b border-neutral-800/50 pb-8">
        <div>
          <h1 className="text-3xl font-black tracking-[-0.05em] text-white">
            ALPHA<span className="text-emerald-500 font-extrabold">-STREAM</span> 
          </h1>
          <p className="text-[10px] text-neutral-600 font-bold tracking-[0.3em] uppercase mt-1">Institutional Data Pipeline</p>
        </div>
        
        <div className="hidden md:flex flex-col items-end gap-1">
          <div className="text-[10px] text-emerald-500 font-mono font-bold flex items-center gap-2">
            <div className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-ping"></div>
            CORE SYSTEMS OPERATIONAL
          </div>
          <div className="text-[10px] text-neutral-600 font-mono uppercase">
            Latency: {'<'} 450ms
          </div>
        </div>
      </header>

      {/* MACRO SECTION */}
      <MacroHeader />

      {/* MAIN GRID: SIGNALS + NEWS */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
        
        {/* LEFT COLUMN: Signal Cards (Spans 8 of 12 columns) */}
        <div className="lg:col-span-8 grid grid-cols-1 md:grid-cols-2 gap-8">
          <SignalCard ticker="BTCUSDT" />
          <SignalCard ticker="ETHUSDT" />
          {/* Add more tickers here if needed */}
        </div>

        {/* RIGHT COLUMN: News Feed (Spans 4 of 12 columns) */}
        <div className="lg:col-span-4 h-[calc(100vh-400px)] sticky top-12">
          <NewsFeed />
        </div>

      </div>
    </div>
  );
}