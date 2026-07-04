using EventSourcing.Core;

namespace Sample.Domain;

public sealed class AccountOpened : DomainEvent
{
    public string AccountHolder { get; init; } = string.Empty;
    public decimal InitialDeposit { get; init; }
}

public sealed class MoneyDeposited : DomainEvent
{
    public decimal Amount { get; init; }
    public string Description { get; init; } = string.Empty;
}

public sealed class MoneyWithdrawn : DomainEvent
{
    public decimal Amount { get; init; }
    public string Description { get; init; } = string.Empty;
}

public sealed class MoneyTransferred : DomainEvent
{
    public string TargetAccountId { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public string Description { get; init; } = string.Empty;
}

public sealed class TransferReceived : DomainEvent
{
    public string SourceAccountId { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public string Description { get; init; } = string.Empty;
}
