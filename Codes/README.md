# CSI201 Networked Quiz Tournament

A command-line multiplayer quiz tournament developed using Python 3.12 and TCP sockets.

The server manages quiz questions, rooms, answer deadlines, scoring and leaderboards. Clients connect to the server to join rooms and participate in quizzes.

## Requirements

- Python 3.12
- Python standard library only
- A valid JSON question bank

## Running the Program

In the examiner-provided Docker environment, open a terminal in the `Codes` directory. Use the required command forms:

```bash
python3 server.py --host <server_IP> --port <port> --questions <json_path>
python3 client.py --server <server_IP> --port <port> --id <user_id> --name <display_name>
```

For example, when the server and clients share the same network namespace:

Start the server:

```bash
python3 server.py --host 127.0.0.1 --port 12000 --questions questions.json
```

The server prints `SERVER_READY` when it is ready to accept connections.

Open another terminal and start a client:

```bash
python3 client.py --server 127.0.0.1 --port 12000 --id user01 --name Alice
```

To start additional clients, use different user IDs and names. For clients in a different network namespace or machine, replace `127.0.0.1` with a reachable server address. To exit, enter `/quit` in each client, then press `Ctrl+C` in the server terminal.

## Client Commands

| Command | Description |
|---|---|
| `/help` | Show available commands |
| `/quizzes` | List available quizzes |
| `/rooms` | List rooms that can be joined |
| `/create <quiz_id>` | Create a room for the selected quiz |
| `/join <room_code>` | Join an existing room |
| `/players` | Show players and their ready status |
| `/ready` | Mark yourself as ready |
| `/unready` | Cancel your ready status |
| `/start` | Start the quiz as room host |
| `/answer A\|B\|C\|D` | Submit an answer |
| `/score` | View the leaderboard |
| `/leave` | Leave the current room |
| `/quit` | Disconnect from the server |

## Tournament Rules

Each room requires at least two players to start and supports a maximum of ten players. All players must be ready before the host can start the quiz.

Each question has its own time limit and point value. Players may submit only one answer per question. Correct answers receive the specified points, while incorrect or unanswered questions receive zero.

After each question closes, the server announces the correct answer, each player's result and the current leaderboard.

Players are ranked by their total scores. Players with equal scores share the same rank. If multiple players have the highest score, they are declared joint winners.

If the host leaves the room, host status transfers to the next remaining player in join order.

After a quiz finishes, players can still view the final leaderboard while they remain in the room. The completed quiz state is removed when the last player leaves or the server shuts down.

## Question Bank

The included `questions.json` contains four quizzes with 35 questions in total.

| Quiz ID | Questions |
|---|---:|
| `networking-basics` | 5 |
| `transport-layer` | 8 |
| `application-layer` | 10 |
| `socket-programming` | 12 |
| **Total** | **35** |

Question time limits and point values are defined in the JSON file.

The server validates the question bank before accepting connections. Another valid question bank can be loaded using the `--questions` argument.

## Testing

Run the automated tests from the `Codes` directory:

```bash
python3 -m unittest discover -s tests -v
```

The current version passed all 92 automated tests in the provided Docker environment using Python 3.12.16.

Docker testing screenshots are available in the repository's `Test_Evidence` directory.

## Assumptions

- Execution and assessment use the examiner-provided Docker environment with Python 3.12 and the Python standard library; no package installation is required.
- The server can read the supplied JSON question bank from the path passed via `--questions`. Relative paths are resolved from the server's working directory.
- Each active player uses a distinct user ID and display name. Server and clients must use a reachable address and the same port.
- Each room supports two to ten players and all members must be ready before the host starts the quiz.

## Known Limitations

- Sessions, rooms and scores are kept in memory and are lost when the server restarts. Reconnecting to an existing session is not supported.
- Network traffic is not encrypted with TLS, and the application does not provide password-based identity verification.
- After a server disconnects, a client waiting for terminal input may require user input or `Ctrl+C` to exit.
- The server limits concurrent connections to 128; successful tests with ten clients do not establish performance at that limit.

## Notes

- The program uses TCP sockets and a length-prefixed JSON message format.
- Question timing and scoring are controlled by the server.
- Each room runs its quiz independently.
- The client does not read the question bank directly.
