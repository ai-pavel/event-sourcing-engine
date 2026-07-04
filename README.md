# Event Sourcing Engine

A C# .NET 8 event sourcing framework with SQLite-backed event store, snapshot support, and a projection engine.

## Features

- **AggregateRoot<TId>** base class with event replay and snapshotting
- **SqliteEventStore** for persisting and loading event streams
- **Projection Engine** for building read models from stored events
- **Sample Domain**: BankAccount with Deposit, Withdraw, and Transfer events

## Structure

- `src/EventSourcing/` — core library (aggregate root, event store, projections)
- `src/Sample/` — sample BankAccount domain with console app
- `tests/EventSourcing.Tests/` — xUnit tests

## Running

```bash
dotnet run --project src/Sample
```

## Testing

```bash
dotnet test
```
