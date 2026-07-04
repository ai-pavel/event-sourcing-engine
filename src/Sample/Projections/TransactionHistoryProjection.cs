using System.Collections.Concurrent;
using EventSourcing.Core;
using EventSourcing.Projections;
using Sample.Domain;

namespace Sample.Projections;

/// <summary>
/// Read-model projection that maintains a chronological transaction history per account.
/// </summary>
public sealed class TransactionHistoryProjection : IProjection
{
    private readonly ConcurrentDictionary<string, List<TransactionRecord>> _history = new();

    public Task HandleAsync(DomainEvent @event)
    {
        switch (@event)
        {
            case AccountOpened e:
                GetOrCreateHistory(e.AggregateId).Add(new TransactionRecord
                {
                    Timestamp = e.Timestamp,
                    Type = "Account Opened",
                    Amount = e.InitialDeposit,
                    Description = $"Account opened for {e.AccountHolder}"
                });
                break;

            case MoneyDeposited e:
                GetOrCreateHistory(e.AggregateId).Add(new TransactionRecord
                {
                    Timestamp = e.Timestamp,
                    Type = "Deposit",
                    Amount = e.Amount,
                    Description = e.Description
                });
                break;

            case MoneyWithdrawn e:
                GetOrCreateHistory(e.AggregateId).Add(new TransactionRecord
                {
                    Timestamp = e.Timestamp,
                    Type = "Withdrawal",
                    Amount = -e.Amount,
                    Description = e.Description
                });
                break;

            case MoneyTransferred e:
                GetOrCreateHistory(e.AggregateId).Add(new TransactionRecord
                {
                    Timestamp = e.Timestamp,
                    Type = "Transfer Out",
                    Amount = -e.Amount,
                    Description = $"To {e.TargetAccountId}: {e.Description}"
                });
                break;

            case TransferReceived e:
                GetOrCreateHistory(e.AggregateId).Add(new TransactionRecord
                {
                    Timestamp = e.Timestamp,
                    Type = "Transfer In",
                    Amount = e.Amount,
                    Description = $"From {e.SourceAccountId}: {e.Description}"
                });
                break;
        }

        return Task.CompletedTask;
    }

    /// <summary>Gets the transaction history for a specific account.</summary>
    public IReadOnlyList<TransactionRecord> GetHistory(string accountId)
    {
        return _history.TryGetValue(accountId, out var history)
            ? history.AsReadOnly()
            : Array.Empty<TransactionRecord>();
    }

    private List<TransactionRecord> GetOrCreateHistory(string accountId)
    {
        return _history.GetOrAdd(accountId, _ => new List<TransactionRecord>());
    }
}

/// <summary>Read-model representing a single transaction entry.</summary>
public sealed class TransactionRecord
{
    public DateTimeOffset Timestamp { get; init; }
    public string Type { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public string Description { get; init; } = string.Empty;

    public override string ToString() =>
        $"  {Timestamp:yyyy-MM-dd HH:mm:ss} | {Type,-15} | {Amount,12:C} | {Description}";
}
