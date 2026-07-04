using Sample.Domain;
using Xunit;

namespace EventSourcing.Tests;

public class BankAccountTests
{
    [Fact]
    public void Open_CreatesAccountWithInitialDeposit()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);

        Assert.Equal("acc-1", account.Id);
        Assert.Equal("Alice", account.AccountHolder);
        Assert.Equal(100m, account.Balance);
        Assert.Equal(1, account.Version);
    }

    [Fact]
    public void Deposit_IncreasesBalance()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);
        account.Deposit(50m, "Paycheck");

        Assert.Equal(150m, account.Balance);
        Assert.Equal(2, account.Version);
    }

    [Fact]
    public void Withdraw_DecreasesBalance()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);
        account.Withdraw(30m, "Coffee");

        Assert.Equal(70m, account.Balance);
    }

    [Fact]
    public void Withdraw_InsufficientFunds_Throws()
    {
        var account = BankAccount.Open("acc-1", "Alice", 50m);

        Assert.Throws<InvalidOperationException>(() =>
            account.Withdraw(100m));
    }

    [Fact]
    public void Transfer_DecreasesSourceBalance()
    {
        var account = BankAccount.Open("acc-1", "Alice", 200m);
        account.TransferTo("acc-2", 75m, "Rent");

        Assert.Equal(125m, account.Balance);
    }

    [Fact]
    public void ReceiveTransfer_IncreasesBalance()
    {
        var account = BankAccount.Open("acc-2", "Bob", 50m);
        account.ReceiveTransfer("acc-1", 75m, "Rent");

        Assert.Equal(125m, account.Balance);
    }

    [Fact]
    public void UncommittedEvents_TracksAllRaisedEvents()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);
        account.Deposit(50m);
        account.Withdraw(20m);

        Assert.Equal(3, account.GetUncommittedEvents().Count);
    }

    [Fact]
    public void ClearUncommittedEvents_EmptiesList()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);
        account.Deposit(50m);
        account.ClearUncommittedEvents();

        Assert.Empty(account.GetUncommittedEvents());
    }

    [Fact]
    public void Snapshot_RoundTrip_RestoresState()
    {
        var account = BankAccount.Open("acc-1", "Alice", 100m);
        account.Deposit(50m);
        account.Withdraw(20m);

        var snapshot = account.CreateSnapshot();

        var restored = new BankAccount();
        restored.RestoreFromSnapshot(snapshot);

        Assert.Equal("acc-1", restored.Id);
        Assert.Equal("Alice", restored.AccountHolder);
        Assert.Equal(130m, restored.Balance);
        Assert.Equal(3, restored.Version);
    }
}
