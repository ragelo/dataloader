# Dataloader

`DataLoader` struct to avoiding N+1 queries when resolving graphql objects.

> N+1 queries are a performance problem in which the application makes database queries in a loop, instead of making a single query that returns or modifies all the information at once. Each database connection takes some amount of time, so querying the database in a loop can be many times slower than doing it just once.
