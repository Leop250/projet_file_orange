import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css"; // L'import indispensable

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "AeroScope",
  description: "Observatoire de la qualité de l'air en Europe",
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