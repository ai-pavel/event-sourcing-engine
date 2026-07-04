using EventSourcing.Projections;
using EventSourcing.Store;
using Sample.Domain;
using Sample.Projections;

// ── Setup ───────────────────────────────────────────────────────────────────

const string connectionString = "Data Source=sample_bank.db";

var typeRegistry = new EventTypeRegistry(typeof(AccountOpened).Assembly);
var eventStore = new SqliteEventStore(connectionString, typeRegistry);
await eventStore.InitializeAsync();

var repository = new AggregateRepository<BankAccount, string>(eventStore, snapshotInterval: 5);

// ── Create accounts ─────────────────────────────────────────────────────────

Console.WriteLine("=== Event Sourcing Engine - Bank Account Sample ===\n");

Console.WriteLine("Opening accounts...");
var alice = BankAccount.Open("ACC-001", "Alice Johnson", 1000m);
await repository.SaveAsync(alice);

var bob = BankAccount.Open("ACC-002", "Bob Smith", 500m);
await repository.SaveAsync(bob);

Console.WriteLine($"  Alice: {alice.AccountHolder}, Balance: {alice.Balance:C}");
Console.WriteLine($"  Bob:   {bob.AccountHolder}, Balance: {bob.Balance:C}");

// ── Perform transactions ────────────────────────────────────────────────────

Console.WriteLine("\nPerforming transactions...");

alice.Deposit(250m, "Paycheck");
alice.Withdraw(100m, "Groceries");
await repository.SaveAsync(alice);

bob.Deposit(300m, "Freelance payment");
await repository.SaveAsync(bob);

// Transfer from Alice to Bob
alice.TransferTo("ACC-002", 200m, "Rent payment");
await repository.SaveAsync(alice);

bob.ReceiveTransfer("ACC-001", 200m, "Rent payment");
await repository.SaveAsync(bob);

Console.WriteLine($"  Alice balance after transactions: {alice.Balance:C}");
Console.WriteLine($"  Bob balance after transactions:   {bob.Balance:C}");

// ── Reload from event store ─────────────────────────────────────────────────

Console.WriteLine("\nReloading aggregates from event store...");
var reloadedAlice = await repository.LoadAsync("ACC-001");
var reloadedBob = await repository.LoadAsync("ACC-002");

Console.WriteLine($"  Alice (reloaded): {reloadedAlice.AccountHolder}, Balance: {reloadedAlice.Balance:C}, Version: {reloadedAlice.Version}");
Console.WriteLine($"  Bob (reloaded):   {reloadedBob.AccountHolder}, Balance: {reloadedBob.Balance:C}, Version: {reloadedBob.Version}");

// ── Build projections ───────────────────────────────────────────────────────

Console.WriteLine("\nBuilding projections from event store...");

var accountSummary = new AccountSummaryProjection();
var transactionHistory = new TransactionHistoryProjection();

var projectionEngine = new ProjectionEngine(eventStore);
projectionEngine
    .Register(accountSummary)
    .Register(transactionHistory);

await projectionEngine.CatchUpAsync();

// ── Display read models ─────────────────────────────────────────────────────

Console.WriteLine("\n--- Account Summaries ---");
foreach (var summary in accountSummary.GetAllAccounts())
{
    Console.WriteLine($"  {summary}");
}

Console.WriteLine("\n--- Alice's Transaction History ---");
foreach (var record in transactionHistory.GetHistory("ACC-001"))
{
    Console.WriteLine(record);
}

Console.WriteLine("\n--- Bob's Transaction History ---");
foreach (var record in transactionHistory.GetHistory("ACC-002"))
{
    Console.WriteLine(record);
}

// ── Demonstrate snapshot ────────────────────────────────────────────────────

Console.WriteLine("\nCreating additional transactions to trigger snapshot...");
for (var i = 0; i < 3; i++)
{
    reloadedAlice.Deposit(10m, $"Small deposit #{i + 1}");
}
await repository.SaveAsync(reloadedAlice);

var snapshot = await eventStore.GetLatestSnapshotAsync("ACC-001");
if (snapshot is not null)
{
    Console.WriteLine($"  Snapshot found for Alice at version {snapshot.Version}");
}
else
{
    Console.WriteLine("  No snapshot yet (will be created at next interval boundary).");
}

// Reload again to verify snapshot path works
var finalAlice = await repository.LoadAsync("ACC-001");
Console.WriteLine($"  Alice (final reload): Balance: {finalAlice.Balance:C}, Version: {finalAlice.Version}");

Console.WriteLine("\nDone.");
