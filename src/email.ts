const RESEND_API_URL = 'https://api.resend.com/emails';

// Resend's shared test sender. Works immediately without verifying a domain;
// swap this for your own verified address (e.g. login@yourdomain.com) once
// you've added and verified a domain in the Resend dashboard.
const FROM_ADDRESS = 'onboarding@resend.dev';

export async function sendLoginCodeEmail(to: string, code: string): Promise<void> {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    throw new Error('RESEND_API_KEY is not configured');
  }

  const response = await fetch(RESEND_API_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: FROM_ADDRESS,
      to,
      subject: `${code} ist dein Anmeldecode`,
      html: `
        <div style="font-family: -apple-system, sans-serif; max-width: 480px; margin: 0 auto;">
          <h2 style="margin-bottom: 8px;">Dein Anmeldecode</h2>
          <p style="color: #444;">Gib diesen Code in der App ein, um dich anzumelden:</p>
          <p style="font-size: 32px; font-weight: bold; letter-spacing: 4px; margin: 24px 0;">${code}</p>
          <p style="color: #888; font-size: 13px;">Der Code ist 10 Minuten gültig. Wenn du diese Anmeldung nicht angefordert hast, kannst du diese E-Mail ignorieren.</p>
        </div>
      `,
      text: `Dein Anmeldecode: ${code}\n\nDer Code ist 10 Minuten gueltig. Wenn du diese Anmeldung nicht angefordert hast, kannst du diese E-Mail ignorieren.`,
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Resend request failed: ${response.status} ${body}`);
  }
}
