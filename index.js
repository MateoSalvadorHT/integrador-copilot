/**
 * Genesys Agent Copilot — Respuestas estáticas
 * Con conversationId dinámico obtenido desde el WebSocket
 */

const WebSocket = require("ws");
const fetch     = require("node-fetch");

// ─── Configuración ────────────────────────────────────────────────────────────

const CONFIG = {
  region:       process.env.GENESYS_REGION       || "mypurecloud.com",
  clientId:     process.env.GENESYS_CLIENT_ID,
  clientSecret: process.env.GENESYS_CLIENT_SECRET,
};

const REQUIRED = ["clientId", "clientSecret"];
const missing  = REQUIRED.filter((k) => !CONFIG[k]);
if (missing.length > 0) {
  console.error("❌ Variables de entorno faltantes:", missing);
  process.exit(1);
}

const BASE_URL = `https://api.${CONFIG.region}`;
const AUTH_URL = `https://login.${CONFIG.region}`;

// ─── Base de conocimiento estática ───────────────────────────────────────────

const KNOWLEDGE_BASE = [
  {
    keywords: ["color", "cielo"],
    answer:   "El cielo es de color azul.",
  },
  {
    keywords: ["capital", "ecuador"],
    answer:   "La capital de Ecuador es Quito.",
  },
  {
    keywords: ["mejor", "bebida"],
    answer:   "La mejor bebida es el café ☕",
  },
];

const DEFAULT_ANSWER = "No tengo esa información.";

function getAnswer(question) {
  const normalized = question
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  const match = KNOWLEDGE_BASE.find((entry) =>
    entry.keywords.every((kw) => normalized.includes(kw))
  );

  return match ? match.answer : DEFAULT_ANSWER;
}

// ─── Genesys: Token OAuth ─────────────────────────────────────────────────────

let currentToken   = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  if (currentToken && Date.now() < tokenExpiresAt - 5 * 60 * 1000) {
    return currentToken;
  }

  const credentials = Buffer.from(
    `${CONFIG.clientId}:${CONFIG.clientSecret}`
  ).toString("base64");

  const response = await fetch(`${AUTH_URL}/oauth/token`, {
    method: "POST",
    headers: {
      Authorization:  `Basic ${credentials}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: "grant_type=client_credentials",
  });

  if (!response.ok) {
    throw new Error(`Auth falló: ${response.status} ${await response.text()}`);
  }

  const data     = await response.json();
  currentToken   = data.access_token;
  tokenExpiresAt = Date.now() + data.expires_in * 1000;

  console.log(`✅ Token obtenido. Expira: ${new Date(tokenExpiresAt).toISOString()}`);
  return currentToken;
}

// ─── Genesys: Obtener userId del agente autenticado ───────────────────────────
// Solo se llama una vez al arrancar para saber a qué usuario suscribirse

async function getMyUserId(token) {
  const response = await fetch(`${BASE_URL}/api/v2/users/me`, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!response.ok) {
    throw new Error(`getMyUserId falló: ${response.status} ${await response.text()}`);
  }

  const data = await response.json();
  console.log(`✅ Usuario identificado: ${data.name} (${data.id})`);
  return data.id;
}

// ─── Genesys: Canal de notificaciones ────────────────────────────────────────

async function createNotificationChannel(token) {
  const response = await fetch(`${BASE_URL}/api/v2/notifications/channels`, {
    method: "POST",
    headers: {
      Authorization:  `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Canal falló: ${response.status} ${await response.text()}`);
  }

  const channel = await response.json();
  console.log("✅ Canal creado:", channel.id);
  return channel;
}

// ─── Genesys: Suscribirse a tópicos ──────────────────────────────────────────

async function subscribeToTopics(token, channelId, topics) {
  const response = await fetch(
    `${BASE_URL}/api/v2/notifications/channels/${channelId}/subscriptions`,
    {
      method: "POST",           // POST agrega sin reemplazar suscripciones previas
      headers: {
        Authorization:  `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(topics.map((id) => ({ id }))),
    }
  );

  if (!response.ok) {
    throw new Error(`Suscripción falló: ${response.status} ${await response.text()}`);
  }

  topics.forEach((t) => console.log("✅ Suscrito:", t));
}

// ─── Genesys: Enviar resultado al agente ──────────────────────────────────────

async function sendActionResult(token, conversationId, suggestionId, answer) {
  const body = {
    state:  "Success",
    result: JSON.stringify({ respuesta: answer }),
  };

  const response = await fetch(
    `${BASE_URL}/api/v2/conversations/${conversationId}/suggestions/${suggestionId}/result`,
    {
      method: "POST",
      headers: {
        Authorization:  `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    }
  );

  if (response.ok) {
    console.log(`✅ Respuesta enviada: "${answer}"`);
  } else {
    console.error("❌ Error enviando resultado:", await response.text());
  }
}

// ─── Manejadores de eventos WebSocket ────────────────────────────────────────

// Conversaciones activas suscritas (para no repetir suscripciones)
const subscribedConversations = new Set();

/**
 * Evento: v2.users.{userId}.conversations
 * Llega cuando el agente entra/sale de una conversación.
 * Usamos esto para suscribirnos dinámicamente al tópico de Copilot.
 */
async function handleUserConversationEvent(event, channelId) {
  const { eventBody } = event;
  const conversationId = eventBody?.id || eventBody?.conversationId;

  if (!conversationId) return;

  // Solo suscribirse una vez por conversación
  if (subscribedConversations.has(conversationId)) return;
  subscribedConversations.add(conversationId);

  console.log(`\n📞 Nueva conversación detectada: ${conversationId}`);
  console.log("   Suscribiendo al tópico de Copilot...");

  try {
    const token = await getAccessToken();
    await subscribeToTopics(token, channelId, [
      `v2.conversations.${conversationId}.suggestions.thirdpartyaction`,
    ]);
  } catch (err) {
    console.error("❌ Error suscribiendo a conversación:", err.message);
    subscribedConversations.delete(conversationId); // permitir reintento
  }
}

/**
 * Evento: v2.conversations.{id}.suggestions.thirdpartyaction
 * Llega cuando Copilot activa una regla de terceros.
 */
async function handleThirdPartyActionEvent(event) {
  const { eventBody } = event;
  const { conversationId, suggestion }        = eventBody;
  const { id: suggestionId, parameters = {} } = suggestion;

  const question = parameters.customerQuestion || parameters.question || "";

  console.log(`\n🎯 Pregunta recibida: "${question}"`);

  const answer = getAnswer(question);
  console.log(`💬 Respuesta: "${answer}"`);

  const token = await getAccessToken();
  await sendActionResult(token, conversationId, suggestionId, answer);
}

/**
 * Router central — decide qué handler usar según el tópico del evento
 */
function routeEvent(event, channelId) {
  const topic = event.topicName || "";

  if (topic.includes("users") && topic.includes("conversations")) {
    handleUserConversationEvent(event, channelId);
    return;
  }

  if (topic.includes("thirdpartyaction")) {
    handleThirdPartyActionEvent(event);
    return;
  }

  // Tópico no reconocido — ignorar silenciosamente
}

// ─── WebSocket: Conexión persistente ─────────────────────────────────────────

function connectWebSocket(connectUri, channelId) {
  console.log("🔌 Conectando WebSocket...");
  const ws = new WebSocket(connectUri);

  ws.on("open", () => {
    console.log("✅ WebSocket conectado\n");

    // Heartbeat cada 30s
    const heartbeat = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ message: "ping" }));
      } else {
        clearInterval(heartbeat);
      }
    }, 30_000);
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.topicName === "channel.metadata") return;
      routeEvent(msg, channelId);
    } catch (err) {
      console.error("Error parseando mensaje:", err.message);
    }
  });

  // Reconexión con backoff exponencial
  let retryDelay = 5_000;
  ws.on("close", (code) => {
    console.warn(`⚠️  WebSocket cerrado (${code}). Reconectando en ${retryDelay / 1000}s...`);
    setTimeout(() => {
      connectWebSocket(connectUri, channelId);
      retryDelay = Math.min(retryDelay * 2, 60_000);
    }, retryDelay);
  });

  ws.on("error", (err) => console.error("❌ WebSocket error:", err.message));
}

// ─── Arranque ─────────────────────────────────────────────────────────────────

async function main() {
  console.log("🚀 Genesys Copilot — Respuestas estáticas (conversationId dinámico)");
  console.log(`   Región: ${CONFIG.region}\n`);

  try {
    const token   = await getAccessToken();

    // 1. Obtener userId para saber a qué usuario suscribirse
    const userId  = await getMyUserId(token);

    // 2. Crear canal WebSocket
    const channel = await createNotificationChannel(token);

    // 3. Suscribirse al tópico de conversaciones del agente
    //    Cada vez que entre una llamada, recibiremos el conversationId
    await subscribeToTopics(token, channel.id, [
      `v2.users.${userId}.conversations`,
    ]);

    // 4. Conectar WebSocket — a partir de aquí todo es reactivo
    connectWebSocket(channel.connectUri, channel.id);

    console.log("🎧 Esperando conversaciones entrantes...\n");
  } catch (err) {
    console.error("❌ Error al arrancar:", err.message);
    process.exit(1);
  }
}

process.on("unhandledRejection", (err) => console.error("❌ unhandledRejection:", err));
process.on("uncaughtException",  (err) => { console.error("❌ uncaughtException:", err); process.exit(1); });

main();