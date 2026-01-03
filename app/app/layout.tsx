import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css"; // L'import indispensable

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Air Observatory",
  description: "Dashboard Temps Réel - BigQuery & Google Maps",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="fr">
      <body 
        className={`${inter.className} bg-black text-white h-screen w-screen overflow-hidden antialiased`}
        style={{ backgroundColor: 'black', color: 'white', margin: 0 }}
      >
        {children}
      </body>
    </html>
  );
}