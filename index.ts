import { serve, json } from "https://deno.land/x/sift@0.3.2/mod.ts";
import SqlString from "https://esm.sh/v41/sqlstring@2.3.2";

const dbProxyPassword = Deno.env.get("DBPROXY_PASSWORD");
if (!dbProxyPassword) {
  throw new Error("Expected environment variable DBPROXY_PASSWORD to be set");
}

const validate = (
  body: unknown
):
  | Error
  | {
      method: "post" | "get";
      url: string;
      headers?: Record<string, string>;
      body?: unknown;
    } => {
  if (typeof body !== "object" || body === null)
    return new Error("Expected body to be an object");
  const json = body as Record<string, unknown>;

  if (typeof json.method !== "string")
    return new Error("Expected body.method to be a string");
  const uncheckedMethod = json.method.toLowerCase();
  if (uncheckedMethod !== "post" && uncheckedMethod !== "get")
    return new Error('Expected body.method to be "POST" or "GET"');
  const method = uncheckedMethod;

  if (typeof json.url !== "string")
    return new Error("Expected body.url to be a string");
  try {
    new URL(json.url);
  } catch {
    return new Error("Expected body.url to be a valid url");
  }
  const url = json.url;

  let headers: Record<string, string> | undefined;
  if ("headers" in json) {
    if (typeof json.headers !== "object" || json.headers === null)
      throw new Error("Expected specified body.headers to be an object");
    const test = json.headers as Record<string, unknown>;
    for (const prop in test)
      if (typeof test[prop] !== "string")
        return new Error(
          `Expected specified body.headers.${prop} to be a string`
        );

    headers = test as Record<string, string>;
  }

  return { url, method, headers, body: json.body };
};

const query = <T = unknown>(query: string, ...args: unknown[]) =>
  fetch("https://w3x.io/sql", {
    method: "POST",
    headers: {
      "x-dbproxy-user": "http_queue_producer",
      "x-dbproxy-password": dbProxyPassword,
    },
    body: SqlString.format(query, args),
  }).then(async (r) => (await r.json()) as T);

const main = async (request: Request): Promise<Response> => {
  if (request.method !== "POST")
    return json({ message: "not found" }, { status: 404 });

  const authorization = request.headers.get("Authorization");
  if (typeof authorization !== "string")
    return json(
      { message: "Expected header Authorization to be set" },
      { status: 401 }
    );
  const [type, key] = authorization.split(" ");
  if (type.toLowerCase() !== "bearer" || typeof key !== "string")
    return json({
      message: 'Expected header Authroization in format "Bearer {key}"',
    });

  const body = validate(await request.json());
  if (body instanceof Error)
    return json({ message: body.message }, { status: 400 });

  const validateKey = await query<[{ exists: number }]>(
    `SELECT EXISTS(
    SELECT *
    FROM w3xio.http_queue_keys
    WHERE
        \`key\` = ?
        AND expired IS NULL
) \`exists\`;`,
    key
  );
  if (validateKey[0].exists !== 1)
    return json({ message: "Invalid or expired key" }, { status: 401 });

  const result = await query<{ insertId: string }>(
    `INSERT INTO w3xio.http_queue (\`key\`, method, url, headers, body, require_ok, retries) VALUES
(?, ?, ?, ?, ?, 1, 20);`,
    key,
    body.method,
    body.url,
    JSON.stringify(body.headers),
    JSON.stringify(body.body)
  );

  return json({ id: result.insertId });
};

serve({
  "/": main,
  404: () => json({ message: "not found" }, { status: 404 }),
});
