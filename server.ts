import 'dotenv/config';
import express, { Request, Response } from 'express';
import cors from 'cors';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
  proto
} from '@whiskeysockets/baileys';
import QRCode from 'qrcode';
import path from 'path';
import fs from 'fs';
import pino from 'pino';

// ============================================================================
// SUPABASE CLIENT CONFIGURATION
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
    console.warn('[Supabase] Credenciales no detectadas (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY).');
    return null;
  }

  try {
    supabaseInstance = createClient(supabaseUrl, supabaseKey);
    return supabaseInstance;
  } catch (err) {
    console.error('[Supabase] Error al inicializar cliente:', err);
    return null;
  }
}

// ============================================================================
// WHATSAPP ON-DEMAND SERVICE CLASS
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

    this.socket = makeWASocket({
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: 'silent' }),
      browser: ['Millennium Academy Admin', 'Chrome', '122.0.0.0']
    });

    this.socket.ev.on('creds.update', saveCreds);

    this.socket.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        this.connectionState = 'awaiting_scan';
        try {
          this.qrCodeDataUrl = await QRCode.toDataURL(qr);
        } catch (qrErr) {
          console.error('[WhatsApp On-Demand] Error al generar código QR en base64:', qrErr);
        }
      }

      if (connection === 'open') {
        this.connectionState = 'connected';
        this.qrCodeDataUrl = null;
        this.connectedPhone = this.socket?.user?.id?.split(':')[0] || null;
        console.log(`[WhatsApp On-Demand] Conectado exitosamente con el teléfono: ${this.connectedPhone}`);
      }

      if (connection === 'close') {
        const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
        const isLoggedOut = statusCode === DisconnectReason.loggedOut;
        this.connectionState = 'disconnected';
        this.qrCodeDataUrl = null;
        this.connectedPhone = null;

        if (isLoggedOut) {
          console.log('[WhatsApp On-Demand] Sesión cerrada permanentemente o desvinculada.');
          this.clearSessionFolder();
        }
      }
    });

    this.socket.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify' && type !== 'append') return;

      for (const msg of messages) {
        await this.persistMessageToSupabase(msg);
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
      if (!supabase) return;

      // 1. Upsert en wa_contacts
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
        console.error('[WhatsApp On-Demand] Error al hacer upsert de contacto:', contactError);
        return;
      }

      // 2. Inserción en wa_messages
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
        console.error('[WhatsApp On-Demand] Error al guardar mensaje en Supabase:', msgError);
      }
    } catch (err) {
      console.error('[WhatsApp On-Demand] Excepción guardando historial en Supabase:', err);
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
// EXPRESS SERVER SETUP
// ============================================================================
const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors({ origin: '*' }));
app.use(express.json());

// Health check para monitoreo en Render
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// Endpoints WhatsApp On-Demand
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
  console.log(`[WhatsApp Microservice] Servidor corriendo en el puerto ${PORT}`);
});
