import express from "express";
import path from "path";
import { SSEServer } from "sse-manager";

interface TennisEvents {
  score_update: {
    matchId: string;
    player1: string;
    player2: string;
    score1: number;
    score2: number;
    set: number;
    serving: string;
  };
}

interface Match {
  id: string;
  player1: string;
  player2: string;
  score1: number;
  score2: number;
  set: number;
  serving: string;
}

const matches: Match[] = [
  { id: "match-1", player1: "Djokovic", player2: "Alcaraz",   score1: 0, score2: 0, set: 1, serving: "Djokovic" },
  { id: "match-2", player1: "Sinner",   player2: "Medvedev",  score1: 0, score2: 0, set: 1, serving: "Sinner" },
  { id: "match-3", player1: "Zverev",   player2: "Rune",      score1: 0, score2: 0, set: 1, serving: "Zverev" },
  { id: "match-4", player1: "Tsitsipas", player2: "Auger-Aliassime", score1: 0, score2: 0, set: 1, serving: "Tsitsipas" },
  { id: "match-5", player1: "Fritz",    player2: "De Minaur", score1: 0, score2: 0, set: 1, serving: "Fritz" },
];

const matchMap = new Map(matches.map((m) => [m.id, m]));

// Tennis score progression: 0 → 15 → 30 → 40 → game
const SCORE_STEPS = [0, 15, 30, 40];

function awardPoint(match: Match, winner: "player1" | "player2") {
  const scoreKey = winner === "player1" ? "score1" : "score2";
  const loserKey = winner === "player1" ? "score2" : "score1";

  const current = SCORE_STEPS.indexOf(match[scoreKey]);
  const loserScore = match[loserKey];

  if (current === 3 && loserScore === 40) {
    // Deuce / advantage — just reset for simplicity
    match.score1 = 0;
    match.score2 = 0;
  } else if (current === 3) {
    // Game won — reset scores, increment set counter
    match.score1 = 0;
    match.score2 = 0;
    match.set += 1;
    // Alternate serve on new game
    match.serving = match.serving === match.player1 ? match.player2 : match.player1;
  } else {
    match[scoreKey] = SCORE_STEPS[current + 1];
  }
}

const app = express();
const sseServer = new SSEServer({ cors: { origin: "*" } });
const tennisNamespace = sseServer.of<TennisEvents>("/tennis");

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "client.html"));
});

app.get("/matches", (_req, res) => {
  res.json(matches);
});

app.get("/stream/:matchId", (req, res) => {
  const match = matchMap.get(req.params.matchId);
  if (!match) {
    res.status(404).end();
    return;
  }

  const client = tennisNamespace.connect(req, res);
  client.join(match.id);
});

// Simulate a point every 2 seconds on a random match
setInterval(() => {
  const match = matches[Math.floor(Math.random() * matches.length)];
  const winner = Math.random() < 0.5 ? "player1" : "player2";
  awardPoint(match, winner);

  tennisNamespace.to(match.id).emit("score_update", {
    matchId: match.id,
    player1: match.player1,
    player2: match.player2,
    score1: match.score1,
    score2: match.score2,
    set: match.set,
    serving: match.serving,
  });
}, 2000);

app.listen(3000, () => {
  console.log("Sports updates running at http://localhost:3000");
  console.log("Open multiple tabs and subscribe to different matches to see rooms in action.");
});
