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
// 1. MANEJO GLOBAL DE ERRORES
// ============================================================================
process.on('uncaughtException', (err) => {
  console.error('[CRITICAL - Uncaught Exception]:', err?.message || err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[CRITICAL - Unhandled Rejection at]:', promise, 'reason:', reason);
});

// ============================================================================
// 2. CONFIGURACIÓN DEL CLIENTE SUPABASE (INICIALIZACIÓN DIFERIDA)
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

// Comprobación preventiva de la columna "lid" en wa_contacts
(async () => {
  try {
    const supabase = getSupabase();
    if (supabase) {
      const check = await supabase.from('wa_contacts').select('lid').limit(1);
      if (check && (check as any).error) {
        console.warn('[WhatsApp] AVISO: Asegúrate de tener la columna "lid" (texto, nullable) en la tabla wa_contacts de Supabase.');
      }
    }
  } catch {
    // Silencioso
  }
})();

// ============================================================================
// 3. CLASE WHATSAPP ON-DEMAND (MAPEO EXACTO DE ESTADOS DE BAILEYS)
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

    // Limpieza preventiva de sockets previos
    if (this.socket) {
      try {
        this.socket.ev.removeAllListeners('connection.update');
        this.socket.ev.removeAllListeners('creds.update');
        this.socket.ev.removeAllListeners('messages.upsert');
        this.socket.ev.removeAllListeners('messages.update');
        this.socket.ev.removeAllListeners('message-receipt.update');
        this.socket.end(undefined);
      } catch (cleanErr) {
        console.warn('[WhatsApp] Advertencia al destruir socket previo:', cleanErr);
      }
      this.socket = null;
    }

    if (this.connectionState !== 'connected') {
      this.connectionState = 'connecting';
    }
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
      // Fallback
    }

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

        // Auto-reconexión silenciosa en micro-cortes
        if (connection === 'close') {
          const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
          const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

          if (shouldReconnect) {
            console.log('[WhatsApp] Micro-corte detectado. Reconectando silenciosamente...');
            this.startSession();
          } else {
            console.log('[WhatsApp] Sesión cerrada permanentemente o desvinculada.');
            this.connectionState = 'disconnected';
            this.qrCodeDataUrl = null;
            this.connectedPhone = null;
            this.clearSessionFolder();
            if (this.socket) {
              try {
                this.socket.end(undefined);
              } catch {}
              this.socket = null;
            }
          }
        }
      } catch (connErr) {
        console.error('[WhatsApp On-Demand] Error en connection.update:', connErr);
      }
    });

    // 1. Listener de mensajes entrantes y salientes
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

    // 2. Listener de Actualización de Mensajes (Status numérico)
    this.socket.ev.on('messages.update', async (updates) => {
      const supabase = getSupabase();
      if (!supabase) return;

      for (const update of updates) {
        if (update.key && update.key.fromMe && update.update && typeof update.update.status === 'number') {
          const externalId = update.key.id;
          const statusNum = update.update.status;

          let statusText: string | null = null;
          // Mapeo actual de Baileys: 2=SERVER_ACK, 3=DELIVERY_ACK, 4=READ, 5=PLAYED
          if (statusNum === 2) statusText = 'sent';
          if (statusNum === 3) statusText = 'delivered';
          if (statusNum === 4 || statusNum === 5) statusText = 'read';

          if (externalId && statusText) {
            try {
              await supabase
                .from('wa_messages')
                .update({ status: statusText })
                .eq('external_id', externalId);
            } catch (e) {
              console.error('[WhatsApp] Error actualizando estado de mensaje:', e);
            }
          }
        }
      }
    });

    // 3. Listener de Acuses de Recibo Nativos
    this.socket.ev.on('message-receipt.update', async (updates) => {
      const supabase = getSupabase();
      if (!supabase) return;

      for (const receipt of updates) {
        if (receipt.key && receipt.key.id && receipt.key.fromMe) {
          const externalId = receipt.key.id;
          const type = (receipt.receipt as any)?.receiptType;

          let statusText = 'delivered'; // Asumimos entregado por defecto si llega recibo
          if (type === 'read' || type === 'read-self' || type === 'played') {
            statusText = 'read';
          }

          try {
            await supabase
              .from('wa_messages')
              .update({ status: statusText })
              .eq('external_id', externalId);
          } catch (e) {
            console.error('[WhatsApp] Error en message-receipt.update:', e);
          }
        }
      }
    });

    return { qrCodeDataUrl: this.qrCodeDataUrl, state: this.connectionState };
  }

  // Envía un mensaje y busca el LID si existe
  public async sendMessage(toPhone: string, text: string) {
    if (!this.socket || this.connectionState !== 'connected') {
      throw new Error('No hay una sesión activa de WhatsApp');
    }

    const cleanPhone = toPhone.replace(/[^\d]/g, '');
    let targetJid = `${cleanPhone}@s.whatsapp.net`;

    try {
      const supabase = getSupabase();
      if (supabase) {
        const { data } = await supabase.from('wa_contacts').select('lid').eq('phone', cleanPhone).single();
        if (data && data.lid) {
          targetJid = data.lid;
        }
      }
    } catch (e) {}

    const sent = await this.socket.sendMessage(targetJid, { text });
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
      // Ignorar si ya estaba cerrado
    }
    this.connectionState = 'disconnected';
    this.qrCodeDataUrl = null;
    this.connectedPhone = null;
    this.clearSessionFolder();
    return { success: true };
  }

  // Guarda en BD con traducción inversa y bloqueo de huérfanos
  private async persistMessageToSupabase(msg: proto.IWebMessageInfo) {
    if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const isOutbound = Boolean(msg.key.fromMe);
    const remoteJid = msg.key.remoteJid || '';

    // Bloqueo de sistema y sincronización
    const myPhone = this.socket?.user?.id?.split(':')[0] || '';
    let rawPhone = remoteJid;
    if (rawPhone.includes(':')) rawPhone = rawPhone.split(':')[0] + rawPhone.substring(rawPhone.indexOf('@'));
    const rawPhoneNum = rawPhone.replace(/[^\d]/g, '');
    if (rawPhoneNum === myPhone || msg.message?.protocolMessage || msg.message?.senderKeyDistributionMessage) return;

    const supabase = getSupabase();
    if (!supabase) return;

    let lidToSave: string | null = null;
    let finalPhone = rawPhoneNum;

    // Lógica de Traducción Bidireccional LID -> Teléfono
    if (remoteJid.includes('@lid')) {
      lidToSave = remoteJid;
      const realPhone = (msg.key as any).senderPn;

      if (realPhone && realPhone.includes('@s.whatsapp.net')) {
        finalPhone = realPhone.replace(/[^\d]/g, '');
      } else {
        try {
          const { data } = await supabase.from('wa_contacts').select('phone').eq('lid', lidToSave).single();
          if (data && data.phone) finalPhone = data.phone;
        } catch (e) {}
      }
    } else {
      finalPhone = remoteJid.replace('@s.whatsapp.net', '').replace(/[^\d]/g, '');
    }

    // SEGURO DE VIDA: Si finalPhone sigue siendo un LID falso, abortamos para no crear chats huérfanos
    if (finalPhone.startsWith('69') && finalPhone.length > 12) {
      console.warn(`[WhatsApp] Ignorando mensaje huérfano sin mapeo para el LID: ${lidToSave}`);
      return;
    }

    const textBody =
      msg.message.conversation ||
      msg.message.extendedTextMessage?.text ||
      msg.message.imageMessage?.caption ||
      msg.message.videoMessage?.caption ||
      '[Multimedia / Archivo adjunto]';

    const pushName = msg.pushName || `+${finalPhone}`;
    const timestamp = msg.messageTimestamp
      ? new Date(Number(msg.messageTimestamp) * 1000).toISOString()
      : new Date().toISOString();

    try {
      const { data: existingContact } = await supabase.from('wa_contacts').select('id').eq('phone', finalPhone).single();
      let contactId: string;

      if (existingContact) {
        contactId = existingContact.id;
        const updateData: any = { last_message_at: timestamp };
        if (lidToSave) updateData.lid = lidToSave;
        await supabase.from('wa_contacts').update(updateData).eq('id', contactId);
      } else {
        const { data: newContact } = await supabase
          .from('wa_contacts')
          .insert([{ phone: finalPhone, name: pushName, last_message_at: timestamp, lid: lidToSave }])
          .select('id')
          .single();
        if (!newContact) return;
        contactId = newContact.id;
      }

      await supabase.from('wa_messages').insert([
        {
          contact_id: contactId,
          message_body: textBody,
          direction: isOutbound ? 'outbound' : 'inbound',
          timestamp: timestamp,
          external_id: msg.key.id || null,
          status: isOutbound ? 'sent' : 'delivered'
        }
      ]);
    } catch (err) {
      console.error('[WhatsApp On-Demand] Error al persistir:', err);
    }
  }

  private clearSessionFolder() {
    try {
      if (fs.existsSync(this.authDir)) {
        fs.rmSync(this.authDir, { recursive: true, force: true });
      }
    } catch {
      // Silencioso
    }
  }
}

const whatsAppOnDemand = new WhatsAppOnDemandService();

// ============================================================================
// 4. EXPRESS SERVER SETUP
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
// 5. RUTAS API
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
