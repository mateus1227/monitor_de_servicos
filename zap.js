const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");

console.log("==================================");
console.log(" INICIANDO WHATSAPP BOT");
console.log("==================================");

const client = new Client({
  authStrategy: new LocalAuth(),
  puppeteer: {
    headless: false, // deixa false para estabilidade
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  }
});

let pronto = false;

// ================= QR
client.on("qr", qr => {
  console.log("ESCANEIE O QR CODE DO WHATSAPP");
  qrcode.generate(qr, { small: true });
});

// ================= AUTH
client.on("authenticated", () => {
  console.log("AUTH OK");
});

client.on("auth_failure", msg => {
  console.log("FALHA AUTH:", msg);
});

// ================= READY
client.on("ready", async () => {
  console.log("WHATSAPP CONECTADO");
  pronto = true;

  // teste automático após iniciar
  setTimeout(async () => {
    await enviarWhatsApp("🚀 TESTE MONITOR OK");
  }, 8000);
});

// ================= FUNÇÃO ENVIAR
async function enviarWhatsApp(msg) {
  try {

    if (!pronto) {
      console.log("WhatsApp ainda não pronto...");
      return;
    }

    // 🔥 NUMERO CORRETO COM 9
    const numero = "seu numero wpp";

    console.log("Buscando número:", numero);

    // tenta achar número
    const numberId = await client.getNumberId(numero);

    if (!numberId) {
      console.log("❌ Número não encontrado no WhatsApp");
      console.log("Abra conversa manual e mande OI primeiro");
      return;
    }

    console.log("Número encontrado:", numberId._serialized);

    // envia
    await client.sendMessage(numberId._serialized, msg);

    console.log("✅ WHATSAPP ENVIADO:", msg);

  } catch (e) {
    console.log("ERRO ENVIO WHATSAPP:", e.message);
  }
}

client.initialize();

module.exports = { enviarWhatsApp };
