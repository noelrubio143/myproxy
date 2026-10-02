// VLESS over WebSocket para sa Deno Deploy

// PALITAN MO ITO ng sarili mong UUID (uuidgenerator.net)
const UUID = "14320000-0000-4000-8000-000000001432";

Deno.serve((req: Request) => {
  if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return new Response("OK", { status: 200 });
  }
  console.log("ws upgrade request", new URL(req.url).pathname);
  const { socket, response } = Deno.upgradeWebSocket(req);
  socket.binaryType = "arraybuffer";
  handleSession(socket);
  return response;
});

function handleSession(ws: WebSocket) {
  let writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  let headerDone = false;
  let chain: Promise<void> = Promise.resolve();

  const onMessage = async (raw: ArrayBuffer) => {
    const data = new Uint8Array(raw);

    if (headerDone) {
      await writer!.write(data);
      return;
    }

    const h = parseHeader(data);
    if (!h || h.command !== 1) { // TCP lang ang suportado
      console.log(
        "rejected: header invalid / UUID mismatch / not TCP. len=",
        data.length,
        "cmd=",
        h?.command,
      );
      safeClose(ws);
      return;
    }
    headerDone = true;

    console.log("connecting to", h.address, h.port);
    const remote = await Deno.connect({ hostname: h.address, port: h.port });
    console.log("connected to", h.address, h.port);
    writer = remote.writable.getWriter();
    pipeRemote(remote, ws, new Uint8Array([h.version, 0]));

    if (h.payload.length) await writer.write(h.payload);
  };

  // chain para sunod-sunod ang pagproseso ng mga mensahe
  ws.onmessage = (e) => {
    if (!(e.data instanceof ArrayBuffer)) return;
    chain = chain.then(() => onMessage(e.data)).catch((err) => {
      console.error("session error:", err);
      safeClose(ws);
    });
  };
  ws.onclose = () => {
    try { writer?.close(); } catch { /* ignore */ }
  };
  ws.onerror = () => safeClose(ws);
}

async function pipeRemote(remote: Deno.TcpConn, ws: WebSocket, header: Uint8Array) {
  let first = true;
  try {
    await remote.readable.pipeTo(new WritableStream({
      write(chunk: Uint8Array) {
        if (ws.readyState !== WebSocket.OPEN) return;
        if (first) {
          const out = new Uint8Array(header.length + chunk.length);
          out.set(header);
          out.set(chunk, header.length);
          ws.send(out);
          first = false;
        } else {
          ws.send(chunk);
        }
      },
    }));
  } catch (err) {
    console.error("pipe error:", err);
  }
  safeClose(ws);
}

function parseHeader(buf: Uint8Array) {
  if (buf.length < 24) return null;
  const version = buf[0];

  const id = Array.from(buf.slice(1, 17))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  if (id !== UUID.replace(/-/g, "").toLowerCase()) return null;

  let i = 18 + buf[17]; // laktawan ang addons
  const command = buf[i++];
  const port = (buf[i] << 8) | buf[i + 1];
  i += 2;

  const type = buf[i++];
  let address: string;
  if (type === 1) {
    address = Array.from(buf.slice(i, i + 4)).join(".");
    i += 4;
  } else if (type === 2) {
    const len = buf[i++];
    address = new TextDecoder().decode(buf.slice(i, i + len));
    i += len;
  } else if (type === 3) {
    const parts: string[] = [];
    for (let k = 0; k < 8; k++) {
      parts.push(((buf[i + k * 2] << 8) | buf[i + k * 2 + 1]).toString(16));
    }
    address = parts.join(":");
    i += 16;
  } else {
    return null;
  }

  return { version, command, port, address, payload: buf.slice(i) };
}

function safeClose(ws: WebSocket) {
  try { ws.close(); } catch { /* ignore */ }
}
