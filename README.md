[![progress-banner](https://backend.codecrafters.io/progress/redis/6bd37c05-2efe-46a1-a075-e0edbd0f571d)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is my solution for the [Build Your Own Redis Challenge](https://codecrafters.io/challenges/redis) by [Codecrafters](https://codecrafters.io/) in Scala 3. I have already [solved this challenge in Go](https://github.com/EshaanAgg/toy-redis), but I wanted to learn Scala practically, and this is one of my favourite networking challenges, so another implementation was in order!

In this challenge, I build a toy Redis clone that's capable of handling basic commands like `PING`, `SET` and `GET`. We then subsequently expand the capacilities of the same via extensions like:
- Replication
- Persistence and RDB formats
- Transactions
- Streaming

All of the implementations are based on the actual Redis protocols and thus are 100% compliant with the same. To find a detailed description of the capacilities of this Redis implementation, head over to the [Codecrafters' course](https://app.codecrafters.io/courses/redis/overview)!

## Running Locally

You can spawn the Redis server instances by using the [`your_program.sh`](./your_program.sh) shell script with the appropiate arguments. You would require to have `sbt` installed to run the Scala code. To test the same, you can use the `redis-cli` command line tool to interact with the server. 