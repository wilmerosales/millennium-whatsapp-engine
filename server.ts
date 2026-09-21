import 'dotenv/config';
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
  proto,
  fetchLatestBaileysVersion
} from '@whiskeysockets/baileys';
import QRCode from 'qrcode';
import path from 'path';
import fs from 'fs';
import pino from 'pino';

// ============================================================================
// 1. MANEJO GLOBAL DE ERRORES (PREVENIR CIERRE ABRUPTO DEL SERVIDOR)
// ============================================================================
process.on('uncaughtException', (err) => {
  console.error('[CRITICAL - Uncaught Exception]:', err?.message || err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[CRITICAL - Unhandled Rejection at]:', promise, 'reason:', reason);
});

// ============================================================================
// 2. SUPABASE CLIENT CONFIGURATION (LAZY INITIALIZATION)
// ============================================================================
let supabaseInstance: SupabaseClient | null = null;

function getSupabase(): SupabaseClient | null {
  if (supabaseInstance) return supabaseInstance;

  const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL || '';
  const supabaseKey =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_ANON_KEY ||
    process.env.VITE_SUPABASE_ANON_KEY ||
    '';

  if (!supabaseUrl || !supabaseKey) {
    console.warn('[Supabase] Credenciales no configuradas (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY).');
    return null;
  }

  try {
    supabaseInstance = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false }
    });
    return supabaseInstance;
  } catch (err) {
    console.error('[Supabase] Error al inicializar cliente:', err);
    return null;
  }
}

// ============================================================================
// 3. CLASE WHATSAPP ON-DEMAND (OPTIMIZADA)
// ============================================================================
class WhatsAppOnDemandService {
  private socket: WASocket | null = null;
  private qrCodeDataUrl: string | null = null;
  private connectionState: 'disconnected' | 'connecting' | 'awaiting_scan' | 'connected' = 'disconnected';
  private connectedPhone: string | null = null;
  private authDir = path.resolve(process.cwd(), '.baileys_auth_ondemand');

  public getStatus() {
    return {
      state: this.connectionState,
      qrCodeDataUrl: this.qrCodeDataUrl,
      connectedPhone: this.connectedPhone,
      timestamp: new Date().toISOString()
    };
  }

  public async startSession(): Promise<{ qrCodeDataUrl: string | null; state: string }> {
    if (this.socket && this.connectionState === 'connected') {
      return { qrCodeDataUrl: null, state: 'connected' };
    }

    this.connectionState = 'connecting';
    this.qrCodeDataUrl = null;

    if (!fs.existsSync(this.authDir)) {
      fs.mkdirSync(this.authDir, { recursive: true });
    }

    const { state, saveCreds } = await useMultiFileAuthState(this.authDir);

    let version: [number, number, number] = [2, 3000, 1015901307];
    try {
      const fetchedVersion = await fetchLatestBaileysVersion();
      if (fetchedVersion?.version) {
        version = fetchedVersion.version;
      }
    } catch {
      // Usar versión por defecto
    }

    // Configuración ligera y segura contra picos de memoria
    this.socket = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: 'silent' }),
      browser: ['Millennium Academy Admin', 'Chrome', '122.0.0.0'],
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      generateHighQualityLinkPreview: false,
      markOnlineOnConnect: false,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
      getMessage: async () => undefined
    });

    this.socket.ev.on('creds.update', saveCreds);

    this.socket.ev.on('connection.update', async (update) => {
      try {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          this.connectionState = 'awaiting_scan';
          try {
            this.qrCodeDataUrl = await QRCode.toDataURL(qr);
          } catch (qrErr) {
            console.error('[WhatsApp On-Demand] Error al generar QR base64:', qrErr);
          }
        }

        if (connection === 'open') {
          this.connectionState = 'connected';
          this.qrCodeDataUrl = null;
          this.connectedPhone = this.socket?.user?.id?.split(':')[0] || null;
          console.log(`[WhatsApp On-Demand] Conectado exitosamente con: ${this.connectedPhone}`);
        }

        if (connection === 'close') {
          const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
          const isLoggedOut = statusCode === DisconnectReason.loggedOut;
          this.connectionState = 'disconnected';
          this.qrCodeDataUrl = null;
          this.connectedPhone = null;

          if (isLoggedOut) {
            console.log('[WhatsApp On-Demand] Sesión desvinculada.');
            this.clearSessionFolder();
          }
        }
      } catch (connErr) {
        console.error('[WhatsApp On-Demand] Error en connection.update:', connErr);
      }
    });

    // 1 & 2: Procesar todos los mensajes y registrar logs de detección
    this.socket.ev.on('messages.upsert', async ({ messages, type }) => {
      for (const msg of messages) {
        console.log('📬 Mensaje detectado de:', msg.key.remoteJid, 'Tipo:', type);
        try {
          await this.persistMessageToSupabase(msg);
        } catch (msgErr) {
          console.error('[WhatsApp On-Demand] Error procesando mensaje:', msgErr);
        }
      }
    });

    return { qrCodeDataUrl: this.qrCodeDataUrl, state: this.connectionState };
  }

  public async sendMessage(toPhone: string, text: string) {
    if (!this.socket || this.connectionState !== 'connected') {
      throw new Error('No hay una sesión activa de WhatsApp');
    }

    const cleanPhone = toPhone.replace(/[^\d]/g, '');
    const jid = `${cleanPhone}@s.whatsapp.net`;

    const sent = await this.socket.sendMessage(jid, { text });
    return sent;
  }

  public async disconnect() {
    try {
      if (this.socket) {
        await this.socket.logout();
        this.socket.end(undefined);
        this.socket = null;
      }
    } catch {
      // Ignorar si el socket ya estaba cerrado
    }
    this.connectionState = 'disconnected';
    this.qrCodeDataUrl = null;
    this.connectedPhone = null;
    this.clearSessionFolder();
    return { success: true };
  }

  // 3: Persistencia estricta en Supabase con captura de errores completa
  private async persistMessageToSupabase(msg: proto.IWebMessageInfo) {
    if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const isOutbound = Boolean(msg.key.fromMe);
    const remoteJid = msg.key.remoteJid || '';
    const rawPhone = remoteJid.replace('@s.whatsapp.net', '').replace(/[^\d]/g, '');

    if (!rawPhone) return;

    const textBody =
      msg.message.conversation ||
      msg.message.extendedTextMessage?.text ||
      msg.message.imageMessage?.caption ||
      msg.message.videoMessage?.caption ||
      '[Multimedia / Archivo adjunto]';

    const pushName = msg.pushName || `+${rawPhone}`;
    const timestamp = msg.messageTimestamp
      ? new Date(Number(msg.messageTimestamp) * 1000).toISOString()
      : new Date().toISOString();

    try {
      const supabase = getSupabase();
      if (!supabase) {
        console.error('❌ Error de Supabase: Cliente Supabase no inicializado (revisa variables de entorno)');
        return;
      }

      // 1. Upsert del contacto
      const { data: contactData, error: contactError } = await supabase
        .from('wa_contacts')
        .upsert(
          {
            phone: rawPhone,
            name: pushName,
            last_message_at: timestamp
          },
          { onConflict: 'phone' }
        )
        .select('id')
        .single();

      if (contactError || !contactData?.id) {
        console.error('❌ Error de Supabase:', contactError || 'No se obtuvo el ID del contacto tras el upsert');
        return;
      }

      // 2. Inserción del mensaje
      const { error: msgError } = await supabase.from('wa_messages').insert([
        {
          contact_id: contactData.id,
          message_body: textBody,
          direction: isOutbound ? 'outbound' : 'inbound',
          timestamp: timestamp,
          external_id: msg.key.id || null,
          status: isOutbound ? 'sent' : 'delivered'
        }
      ]);

      if (msgError) {
        console.error('❌ Error de Supabase:', msgError);
      } else {
        console.log(`✅ Mensaje guardado en Supabase para +${rawPhone} (${isOutbound ? 'Saliente' : 'Entrante'})`);
      }
    } catch (err) {
      console.error('❌ Error de Supabase (Excepción):', err);
    }
  }

  private clearSessionFolder() {
    try {
      if (fs.existsSync(this.authDir)) {
        fs.rmSync(this.authDir, { recursive: true, force: true });
      }
    } catch {
      // Ignorar excepciones al vaciar la carpeta temporal
    }
  }
}

const whatsAppOnDemand = new WhatsAppOnDemandService();

// ============================================================================
// 4. EXPRESS SERVER & CORS PERMISIVO
// ============================================================================
const app = express();
const PORT = process.env.PORT || 3001;

app.use(
  cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept'],
    credentials: false
  })
);

app.use((req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }
  next();
});

app.use(express.json());

app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// ============================================================================
// 5. RUTAS DE LA API
// ============================================================================
app.get('/api/whatsapp/on-demand/status', (_req: Request, res: Response) => {
  res.json(whatsAppOnDemand.getStatus());
});

app.post('/api/whatsapp/on-demand/start', async (_req: Request, res: Response) => {
  try {
    const session = await whatsAppOnDemand.startSession();
    res.json({ success: true, ...session, ...whatsAppOnDemand.getStatus() });
  } catch (err: any) {
    console.error('[API /start Error]:', err);
    res.status(500).json({ success: false, error: err?.message || 'Error al iniciar sesión' });
  }
});

app.post('/api/whatsapp/on-demand/disconnect', async (_req: Request, res: Response) => {
  try {
    const result = await whatsAppOnDemand.disconnect();
    res.json(result);
  } catch (err: any) {
    console.error('[API /disconnect Error]:', err);
    res.status(500).json({ success: false, error: err?.message || 'Error al desconectar' });
  }
});

app.post('/api/whatsapp/on-demand/send', async (req: Request, res: Response) => {
  try {
    const { phone, message } = req.body;
    if (!phone || !message) {
      return res.status(400).json({ success: false, error: 'Campos phone y message son requeridos' });
    }
    const result = await whatsAppOnDemand.sendMessage(phone, message);
    res.json({ success: true, result });
  } catch (err: any) {
    console.error('[API /send Error]:', err);
    res.status(500).json({ success: false, error: err?.message || 'Error al enviar mensaje' });
  }
});

app.listen(PORT, () => {
  console.log(`[WhatsApp Microservice] Servidor escuchando en el puerto ${PORT}`);
});
