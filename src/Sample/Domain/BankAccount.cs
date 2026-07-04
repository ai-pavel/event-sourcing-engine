using System.Text.Json;
using EventSourcing.Core;

namespace Sample.Domain;

/// <summary>
/// Event-sourced aggregate representing a bank account.
/// All state mutations happen through domain events.
/// </summary>
public sealed class BankAccount : AggregateRoot<string>
{
    public string AccountHolder { get; private set; } = string.Empty;
    public decimal Balance { get; private set; }

    // Parameterless constructor required for reconstitution
    public BankAccount() { }

    /// <summary>Opens a new bank account with an initial deposit.</summary>
    public static BankAccount Open(string accountId, string accountHolder, decimal initialDeposit)
    {
        if (initialDeposit < 0)
            throw new InvalidOperationException("Initial deposit cannot be negative.");

        var account = new BankAccount();
        account.RaiseEvent(new AccountOpened
        {
            AggregateId = accountId,
            AccountHolder = accountHolder,
            InitialDeposit = initialDeposit
        });

        return account;
    }

    /// <summary>Deposits money into the account.</summary>
    public void Deposit(decimal amount, string description = "")
    {
        if (amount <= 0)
            throw new InvalidOperationException("Deposit amount must be positive.");

        RaiseEvent(new MoneyDeposited
        {
            AggregateId = Id,
            Amount = amount,
            Description = description
        });
    }

    /// <summary>Withdraws money from the account.</summary>
    public void Withdraw(decimal amount, string description = "")
    {
        if (amount <= 0)
            throw new InvalidOperationException("Withdrawal amount must be positive.");
        if (amount > Balance)
            throw new InvalidOperationException($"Insufficient funds. Balance: {Balance}, Requested: {amount}.");

        RaiseEvent(new MoneyWithdrawn
        {
            AggregateId = Id,
            Amount = amount,
            Description = description
        });
    }

    /// <summary>Transfers money to another account (outbound side).</summary>
    public void TransferTo(string targetAccountId, decimal amount, string description = "")
    {
        if (amount <= 0)
            throw new InvalidOperationException("Transfer amount must be positive.");
        if (amount > Balance)
            throw new InvalidOperationException($"Insufficient funds. Balance: {Balance}, Requested: {amount}.");

        RaiseEvent(new MoneyTransferred
        {
            AggregateId = Id,
            TargetAccountId = targetAccountId,
            Amount = amount,
            Description = description
        });
    }

    /// <summary>Receives a transfer from another account (inbound side).</summary>
    public void ReceiveTransfer(string sourceAccountId, decimal amount, string description = "")
    {
        RaiseEvent(new TransferReceived
        {
            AggregateId = Id,
            SourceAccountId = sourceAccountId,
            Amount = amount,
            Description = description
        });
    }

    protected override void ApplyEvent(DomainEvent @event)
    {
        switch (@event)
        {
            case AccountOpened e:
                Id = e.AggregateId;
                AccountHolder = e.AccountHolder;
                Balance = e.InitialDeposit;
                break;

            case MoneyDeposited e:
                Balance += e.Amount;
                break;

            case MoneyWithdrawn e:
                Balance -= e.Amount;
                break;

            case MoneyTransferred e:
                Balance -= e.Amount;
                break;

            case TransferReceived e:
                Balance += e.Amount;
                break;
        }
    }

    protected override void DeserializeState(string json)
    {
        var state = JsonSerializer.Deserialize<BankAccountState>(json);
        if (state is null) return;

        Id = state.Id;
        AccountHolder = state.AccountHolder;
        Balance = state.Balance;
    }

    protected override string SerializeState()
    {
        var state = new BankAccountState
        {
            Id = Id,
            AccountHolder = AccountHolder,
            Balance = Balance
        };
        return JsonSerializer.Serialize(state);
    }

    /// <summary>Internal DTO for snapshot serialization.</summary>
    private sealed class BankAccountState
    {
        public string Id { get; set; } = string.Empty;
        public string AccountHolder { get; set; } = string.Empty;
        public decimal Balance { get; set; }
    }
}
