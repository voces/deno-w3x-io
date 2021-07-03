import { main } from "./api/main.ts";
import { json, serve } from "./deps.ts";

serve({
  "/": main,
  404: () => json({ message: "not found" }, { status: 404 }),
});
