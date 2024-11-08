using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CryptoExchange.Net.Interfaces;
using CryptoExchange.Net.Logging.Extensions;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Options;
using CryptoExchange.Net.Objects.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CryptoExchange.Net.OrderBook
{
    /// <summary>
    /// Base for order book implementations
    /// </summary>
    public abstract class SymbolOrderBookBBO<T> : ISymbolOrderBook, IDisposable
    {
        private readonly object _bookLock = new object();

        private OrderBookStatus _status;
        private UpdateSubscription? _subscription;
        
        private bool _stopProcessing;
        private Task? _processTask;
        private CancellationTokenSource? _cts;

        private readonly AsyncResetEvent _queueEvent;
        private readonly ConcurrentQueue<T> _processQueue;

        private class EmptySymbolOrderBookEntry : ISymbolOrderBookEntry
        {
            public decimal Quantity { get => 0m;
                set { } }
            public decimal Price { get => 0m;
                set { } }
            public override string ToString()
            {
                return $"{Price,10}x{Quantity,10}";
            }
        }

        private static readonly ISymbolOrderBookEntry _emptySymbolOrderBookEntry = new EmptySymbolOrderBookEntry();

        /// <summary>
        /// The ask list, should only be accessed using the bookLock
        /// </summary>
        protected ISymbolOrderBookEntry? _ask;

        /// <summary>
        /// The bid list, should only be accessed using the bookLock
        /// </summary>
        protected ISymbolOrderBookEntry? _bid;

        /// <summary>
        /// The log
        /// </summary>
        protected ILogger _logger;

        /// <summary>
        /// Whether update numbers are consecutive. If set to true and an update comes in which isn't the previous sequences number + 1
        /// the book will resynchronize as it is deemed out of sync
        /// </summary>
        protected bool _sequencesAreConsecutive;
        
        /// <summary>
        /// Whether levels should be strictly enforced. For example, when an order book has 25 levels and a new update comes in which pushes
        /// the current level 25 ask out of the top 25, should the curent the level 26 entry be removed from the book or does the 
        /// server handle this
        /// </summary>
        protected bool _strictLevels;

        /// <summary>
        /// If the initial snapshot of the book has been set
        /// </summary>
        protected bool _bookSet;

        /// <summary>
        /// The amount of levels for this book
        /// </summary>
        protected int? Levels { get; set; } = null;

        /// <inheritdoc/>
        public string Exchange { get; }

        /// <inheritdoc/>
        public string Api { get; }

        /// <inheritdoc/>
        public OrderBookStatus Status 
        {
            get => _status;
            set
            {
                if (value == _status)
                    return;

                var old = _status;
                _status = value;
                _logger.OrderBookStatusChanged(Api, Symbol, old, value);
                OnStatusChange?.Invoke(old, _status);
            }
        }

        /// <inheritdoc/>
        public long LastSequenceNumber { get; private set; }

        /// <inheritdoc/>
        public string Symbol { get; }

        /// <inheritdoc/>
        public event Action<OrderBookStatus, OrderBookStatus>? OnStatusChange;

        /// <inheritdoc/>
        public event Action<(ISymbolOrderBookEntry BestBid, ISymbolOrderBookEntry BestAsk)>? OnBestOffersChanged;

        /// <inheritdoc/>
        public event Action<(IEnumerable<ISymbolOrderBookEntry> Bids, IEnumerable<ISymbolOrderBookEntry> Asks)>? OnOrderBookUpdate;

        /// <inheritdoc/>
        public DateTime UpdateTime { get; private set; }

        /// <inheritdoc/>
        public int AskCount { get; private set; }

        /// <inheritdoc/>
        public int BidCount { get; private set; }

        /// <inheritdoc/>
        public IEnumerable<ISymbolOrderBookEntry> Asks
        {
            get
            {
                lock (_bookLock)
                {
                    return _ask != null ? new List<ISymbolOrderBookEntry>() { _ask }  : new List<ISymbolOrderBookEntry>();
                }
            }
        }

        /// <inheritdoc/>
        public IEnumerable<ISymbolOrderBookEntry> Bids 
        {
            get
            {
                lock (_bookLock)
                    return _bid != null ? new List<ISymbolOrderBookEntry>() { _bid } : new List<ISymbolOrderBookEntry>();
            }
        }

        /// <inheritdoc/>
        public (IEnumerable<ISymbolOrderBookEntry> bids, IEnumerable<ISymbolOrderBookEntry> asks) Book
        {
            get
            {
                lock (_bookLock)
                    return (Bids, Asks);
            }
        }

        /// <inheritdoc/>
        public ISymbolOrderBookEntry BestBid
        {
            get
            {
                lock (_bookLock)
                    return _bid ?? _emptySymbolOrderBookEntry;
            }
        }

        /// <inheritdoc/>
        public ISymbolOrderBookEntry BestAsk 
        {
            get
            {
                lock (_bookLock)
                    return _ask ?? _emptySymbolOrderBookEntry;
            }
        }

        /// <inheritdoc/>
        public (ISymbolOrderBookEntry Bid, ISymbolOrderBookEntry Ask) BestOffers {
            get {
                lock (_bookLock)
                    return (BestBid,BestAsk);
            }
        }

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="logger">Logger to use. If not provided will create a TraceLogger</param>
        /// <param name="exchange">The exchange of the order book</param>
        /// <param name="api">The API the book is for, for example Spot</param>
        /// <param name="symbol">The symbol the order book is for</param>
        protected SymbolOrderBookBBO(ILoggerFactory? logger, string exchange, string api, string symbol)
        {
            if (symbol == null)
                throw new ArgumentNullException(nameof(symbol));

            Exchange = exchange;
            Api = api;

            _processQueue = new ConcurrentQueue<T>();
            _queueEvent = new AsyncResetEvent(false, true);

            Symbol = symbol;
            Status = OrderBookStatus.Disconnected;

            _ask = null;
            _bid = null;

            _logger = logger?.CreateLogger(Exchange) ?? NullLoggerFactory.Instance.CreateLogger(Exchange);
        }

        /// <summary>
        /// Initialize the order book using the provided options
        /// </summary>
        /// <param name="options">The options</param>
        /// <exception cref="ArgumentNullException"></exception>
        protected void Initialize(OrderBookOptions options)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc/>
        public async Task<CallResult<bool>> StartAsync(CancellationToken? ct = null)
        {
            if (Status != OrderBookStatus.Disconnected)
                throw new InvalidOperationException($"Can't start book unless state is {OrderBookStatus.Disconnected}. Current state: {Status}");

            _logger.OrderBookStarting(Api, Symbol);
            _cts = new CancellationTokenSource();
            ct?.Register(async () =>
            {
                _cts.Cancel();
                await StopAsync().ConfigureAwait(false);
            }, false);

            // Clear any previous messages
            while (_processQueue.TryDequeue(out _)) { }
            _bookSet = false;

            Status = OrderBookStatus.Connecting;
            _processTask = Task.Factory.StartNew(ProcessQueue, TaskCreationOptions.LongRunning);

            var startResult = await DoStartAsync(_cts.Token).ConfigureAwait(false);
            if (!startResult)
            {
                Status = OrderBookStatus.Disconnected;
                return new CallResult<bool>(startResult.Error!);
            }

            if (_cts.IsCancellationRequested)
            {
                _logger.OrderBookStoppedStarting(Api, Symbol);
                await startResult.Data.CloseAsync().ConfigureAwait(false);
                Status = OrderBookStatus.Disconnected;
                return new CallResult<bool>(new CancellationRequestedError());
            }

            _subscription = startResult.Data;
            _subscription.ConnectionLost += HandleConnectionLost;
            _subscription.ConnectionClosed += HandleConnectionClosed;
            _subscription.ConnectionRestored += HandleConnectionRestored;

            Status = OrderBookStatus.Synced;
            return new CallResult<bool>(true);
        }

        private void HandleConnectionLost() 
        {
            _logger.OrderBookConnectionLost(Api, Symbol);    
            if (Status != OrderBookStatus.Disposed) {
                Status = OrderBookStatus.Reconnecting;
                Reset();
            }
        }

        private void HandleConnectionClosed() {
            _logger.OrderBookDisconnected(Api, Symbol);
            Status = OrderBookStatus.Disconnected;
            _ = StopAsync();
        }

        private async void HandleConnectionRestored(TimeSpan _) {
            await ResyncAsync().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task StopAsync()
        {
            _logger.OrderBookStopping(Api, Symbol);
            Status = OrderBookStatus.Disconnected;
            _cts?.Cancel();
            _queueEvent.Set();
            if (_processTask != null)
                await _processTask.ConfigureAwait(false);

            if (_subscription != null) {
                await _subscription.CloseAsync().ConfigureAwait(false);
                _subscription.ConnectionLost -= HandleConnectionLost;
                _subscription.ConnectionClosed -= HandleConnectionClosed;
                _subscription.ConnectionRestored -= HandleConnectionRestored;
            }

            _logger.OrderBookStopped(Api, Symbol);
        }

        /// <inheritdoc/>
        public CallResult<decimal> CalculateAverageFillPrice(decimal baseQuantity, OrderBookEntryType type)
        {
            return new CallResult<decimal>(new InvalidOperationError("UnSupported operation with BBO orderbook"));
        }

        /// <inheritdoc/>
        public CallResult<decimal> CalculateTradableAmount(decimal quoteQuantity, OrderBookEntryType type)
        {
            if (Status != OrderBookStatus.Synced)
                return new CallResult<decimal>(new InvalidOperationError($"{nameof(CalculateTradableAmount)} is not available when book is not in Synced state"));

            return new CallResult<decimal>(new InvalidOperationError("UnSupported operation with BBO orderbook"));

        }

        /// <summary>
        /// Implementation for starting the order book. Should typically have logic for subscribing to the update stream and retrieving
        /// and setting the initial order book
        /// </summary>
        /// <returns></returns>
        protected abstract Task<CallResult<UpdateSubscription>> DoStartAsync(CancellationToken ct);

        /// <summary>
        /// Reset the order book
        /// </summary>
        protected virtual void DoReset() { }

        /// <summary>
        /// Resync the order book
        /// </summary>
        /// <returns></returns>
        protected abstract Task<CallResult<bool>> DoResyncAsync(CancellationToken ct);

        /// <summary>
        /// Set the initial data for the order book. Typically the snapshot which was requested from the Rest API, or the first snapshot
        /// received from a socket subcription
        /// </summary>
        /// <param name="snapshot">snapshot update</param>
        protected void AddSnapshotUpdate(T snapshot)
        {
            _processQueue.Enqueue(snapshot);
            _queueEvent.Set();
        }
 
        /// <summary>
        /// Wait until the order book snapshot has been set
        /// </summary>
        /// <param name="timeout">Max wait time</param>
        /// <param name="ct">Cancellation token</param>
        /// <returns></returns>
        protected async Task<CallResult<bool>> WaitForSetOrderBookAsync(TimeSpan timeout, CancellationToken ct)
        {
            var startWait = DateTime.UtcNow;
            while (!_bookSet && Status == OrderBookStatus.Syncing)
            {
                if(ct.IsCancellationRequested)
                    return new CallResult<bool>(new CancellationRequestedError());

                if (DateTime.UtcNow - startWait > timeout)
                    return new CallResult<bool>(new ServerError("Timeout while waiting for data"));

                try
                {
                    await Task.Delay(50, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                { }
            }

            return new CallResult<bool>(true);
        }

        /// <summary>
        /// IDisposable implementation for the order book
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose method
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            Status = OrderBookStatus.Disposing;

            _cts?.Cancel();
            _queueEvent.Set();

            // Clear queue
            while (_processQueue.TryDequeue(out _)) { }

            _ask = null;
            _bid = null;
            AskCount = 0;
            BidCount = 0;

            Status = OrderBookStatus.Disposed;
        }

        /// <summary>
        /// String representation of the top 3 entries
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ToString(3);
        }

        /// <summary>
        /// String representation of the top x entries
        /// </summary>
        /// <returns></returns>
        public string ToString(int numberOfEntries)
        {
            var stringBuilder = new StringBuilder();
            var book = Book;
            stringBuilder.AppendLine($"   Ask quantity       Ask price | Bid price       Bid quantity");
            for(var i = 0; i < numberOfEntries; i++)
            {
                var ask = book.asks.Count() > i ? book.asks.ElementAt(i): null;
                var bid = book.bids.Count() > i ? book.bids.ElementAt(i): null;
                stringBuilder.AppendLine($"[{ask?.Quantity.ToString(CultureInfo.InvariantCulture),14}] {ask?.Price.ToString(CultureInfo.InvariantCulture),14} | {bid?.Price.ToString(CultureInfo.InvariantCulture),-14} [{bid?.Quantity.ToString(CultureInfo.InvariantCulture),-14}]");
            }
            return stringBuilder.ToString();
        }

        private void CheckBestOffersChanged(ISymbolOrderBookEntry prevBestBid, ISymbolOrderBookEntry prevBestAsk)
        {
            var (bestBid, bestAsk) = BestOffers;
            if (bestBid.Price != prevBestBid.Price || bestBid.Quantity != prevBestBid.Quantity ||
                   bestAsk.Price != prevBestAsk.Price || bestAsk.Quantity != prevBestAsk.Quantity)
            {
                OnBestOffersChanged?.Invoke((bestBid, bestAsk));
            }
        }

        private void Reset()
        {
            _queueEvent.Set();
            // Clear queue
            while (_processQueue.TryDequeue(out _)) { }
            _bookSet = false;
            DoReset();
        }

        private async Task ResyncAsync()
        {
            Status = OrderBookStatus.Syncing;
            var success = false;
            while (!success)
            {
                if (Status != OrderBookStatus.Syncing)
                    return;

                var resyncResult = await DoResyncAsync(_cts!.Token).ConfigureAwait(false);
                success = resyncResult;
            }

            _logger.OrderBookResynced(Api, Symbol);
            Status = OrderBookStatus.Synced;
        }

        private async Task ProcessQueue()
        {
            while (Status != OrderBookStatus.Disconnected && Status != OrderBookStatus.Disposed)
            {
                await _queueEvent.WaitAsync().ConfigureAwait(false);

                while (_processQueue.TryDequeue(out var item))
                {
                    if (Status == OrderBookStatus.Disconnected || Status == OrderBookStatus.Disposed)
                        break;

                    if (_stopProcessing)
                    {
                        _logger.OrderBookMessageSkippedResubscribing(Api, Symbol);
                        continue;
                    }

                    ProcessSnapshotUpdate(item);
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="snapshot"></param>
        protected void ProcessSnapshotUpdate(T snapshot)
        {
            lock (_bookLock)
            {
                _bookSet = true;
                bool changed = Update(snapshot);
                AskCount = _ask == null ? 0 : 1;
                BidCount = _bid == null ? 0 : 1;
                UpdateTime = DateTime.UtcNow;
                _logger.OrderBookDataSet(Api, Symbol, BidCount, AskCount, 0);
                if (changed)
                    OnBestOffersChanged?.Invoke((BestBid, BestAsk));
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Update"></param>
        /// <returns></returns>
        protected abstract bool Update(T Update);
    }
}
