# CSI201 – Networked Quiz Tournament

**Group 26 | Coursework I | Introduction to Networking**

A multiplayer command-line quiz tournament built with Python 3.12 and raw TCP sockets using only the Python standard library. The server manages room membership, question timing, answer validation, scoring and leaderboards. The system supports multiple independent rooms, host transfer and joint winners.

## Repository Structure

- [Codes/](Codes/) — server, client, JSON question bank and automated tests.
- [Documentation/](Documentation/) — development documentation.
- [Test_Evidence/](Test_Evidence/) — Docker test evidence and CLI screenshots.

## Running the Project

Use the examiner-provided Docker environment. Follow the launch commands, client commands and configuration instructions in [Codes/README.md](Codes/README.md).

## Testing

The final code passed all 92 automated tests in the course Docker environment on 26 November 2026. The targeted ten-client and room-isolation checks also passed (2/2; these are part of the 92-test suite).

- [Full automated test results](Test_Evidence/E02_Tests.png)
- [Concurrency test results](Test_Evidence/E09_Concurrency.png)

The submitted question bank contains four quizzes and 35 questions.

## Team

Zihan Gao, Sichen Yu, Jingyue Chen and Jiahe Li.
