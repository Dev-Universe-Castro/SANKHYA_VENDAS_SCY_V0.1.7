// Carregar variáveis de ambiente do arquivo local (se existir)
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env.local');

if (fs.existsSync(envPath)) {
  require('dotenv').config({ path: envPath });
  console.log('✅ Variáveis carregadas de .env.local');
} else {
  console.log('⚠️ Arquivo .env.local não encontrado, usando variáveis do sistema');
}

// Validar variáveis críticas
const requiredVars = [
  'SANKHYA_TOKEN',
  'SANKHYA_APPKEY', 
  'SANKHYA_USERNAME',
  'SANKHYA_PASSWORD'
];

const missingVars = requiredVars.filter(v => !process.env[v]);
if (missingVars.length > 0) {
  console.error('❌ ERRO: Variáveis de ambiente obrigatórias não encontradas:', missingVars);
  console.error('Configure as variáveis no sistema ou crie o arquivo config.env.local');
  process.exit(1);
}

module.exports = {
  apps : [{
    name: "SankhyaVendas",
    script: "node_modules/next/dist/bin/next",
    args: "start -p 5000 -H 0.0.0.0",
    instances: 1,
    exec_mode: "fork",
    watch: false,
    env: {
      NODE_ENV: "production",
      SANKHYA_TOKEN: process.env.SANKHYA_TOKEN,
      SANKHYA_APPKEY: process.env.SANKHYA_APPKEY,
      SANKHYA_USERNAME: process.env.SANKHYA_USERNAME,
      SANKHYA_PASSWORD: process.env.SANKHYA_PASSWORD,
      GEMINI_API_KEY: process.env.GEMINI_API_KEY,
      ORACLE_USER: process.env.ORACLE_USER,
      ORACLE_PASSWORD: process.env.ORACLE_PASSWORD,
      ORACLE_CONNECT_STRING: process.env.ORACLE_CONNECT_STRING,
      NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL_URL
    }
  }]
};
