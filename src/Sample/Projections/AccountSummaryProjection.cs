using System.Collections.Concurrent;
using EventSourcing.Core;
using EventSourcing.Projections;
using Sample.Domain;

namespace Sample.Projections;

/// <summary>
/// Read-model projection that maintains a summary view of all bank accounts.
/// </summary>
public sealed class AccountSummaryProjection : IProjection
{
    private readonly ConcurrentDictionary<string, AccountSummary> _accounts = new();

    public Task HandleAsync(DomainEvent @event)
    {
        switch (@event)
        {
            case AccountOpened e:
                _accounts[e.AggregateId] = new AccountSummary
                {
                    AccountId = e.AggregateId,
                    AccountHolder = e.AccountHolder,
                    Balance = e.InitialDeposit,
                    TransactionCount = 1
                };
                break;

            case MoneyDeposited e:
                if (_accounts.TryGetValue(e.AggregateId, out var depositAccount))
                {
                    depositAccount.Balance += e.Amount;
                    depositAccount.TransactionCount++;
                }
                break;

            case MoneyWithdrawn e:
                if (_accounts.TryGetValue(e.AggregateId, out var withdrawAccount))
                {
                    withdrawAccount.Balance -= e.Amount;
                    withdrawAccount.TransactionCount++;
                }
                break;

            case MoneyTransferred e:
                if (_accounts.TryGetValue(e.AggregateId, out var transferAccount))
                {
                    transferAccount.Balance -= e.Amount;
                    transferAccount.TransactionCount++;
                }
                break;

            case TransferReceived e:
                if (_accounts.TryGetValue(e.AggregateId, out var receiveAccount))
                {
                    receiveAccount.Balance += e.Amount;
                    receiveAccount.TransactionCount++;
                }
                break;
        }

        return Task.CompletedTask;
    }

    /// <summary>Gets the summary for a specific account.</summary>
    public AccountSummary? GetAccount(string accountId)
    {
        return _accounts.GetValueOrDefault(accountId);
    }

    /// <summary>Gets summaries for all known accounts.</summary>
    public IReadOnlyCollection<AccountSummary> GetAllAccounts()
    {
        return _accounts.Values.ToList().AsReadOnly();
    }
}

/// <summary>Read-model representing a summary of a bank account.</summary>
public sealed class AccountSummary
{
    public string AccountId { get; set; } = string.Empty;
    public string AccountHolder { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public int TransactionCount { get; set; }

    public override string ToString() =>
        $"[{AccountId}] {AccountHolder} | Balance: {Balance:C} | Transactions: {TransactionCount}";
}
