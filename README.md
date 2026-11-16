# Networked Quiz Tournament

**CSI201 – Introduction to Networking | Group 26**

A command-line multiplayer quiz tournament built with **Python 3.12** and **TCP sockets**. Players can join rooms, answer timed questions, and view scores and joint winners. The server manages room state, deadlines, scoring, and a replaceable JSON question bank.

## Repository Structure

- [`Codes/`](Codes/) — Python source code, question bank, unit tests, and detailed running instructions.
- [`Documentation/`](Documentation/) — Final development documentation (PDF).
- [`Test_Evidence/`](Test_Evidence/) — Docker and functional test screenshots.

## Quick Start

From the `Codes/` directory, start the server:

```bash
python3 server.py --host 127.0.0.1 --port 12000 --questions questions.json
```

In another terminal, start a client (use a different ID and name for each player):

```bash
python3 client.py --server 127.0.0.1 --port 12000 --id user01 --name Alice
```

Run the automated tests:

```bash
python3 -m unittest discover -s tests -v
```

**Verification:** All **92 automated tests passed**. The project was also exercised in the course-provided Docker environment. See [`Codes/README.md`](Codes/README.md) for full instructions.

## Team

Zihan Gao · Sichen Yu · Jingyue Chen · Jiahe Li
