/**
 * Run this once to generate PWA icons:
 *   node generate-icons.js
 * Requires: npm install canvas
 */

const { createCanvas } = require('canvas');
const fs = require('fs');
const path = require('path');

function makeIcon(size) {
  const canvas = createCanvas(size, size);
  const ctx = canvas.getContext('2d');

  // Background
  ctx.fillStyle = '#0a0a0a';
  ctx.fillRect(0, 0, size, size);

  // Gold circle
  ctx.beginPath();
  ctx.arc(size/2, size/2, size * 0.38, 0, Math.PI * 2);
  ctx.fillStyle = '#c9a84c';
  ctx.fill();

  // Letter L
  ctx.fillStyle = '#000';
  ctx.font = `bold ${size * 0.42}px serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('L', size / 2, size / 2 + size * 0.02);

  const dir = path.join(__dirname, 'icons');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);
  fs.writeFileSync(path.join(dir, `icon-${size}.png`), canvas.toBuffer('image/png'));
  console.log(`✓ icon-${size}.png`);
}

makeIcon(192);
makeIcon(512);
console.log('Icons saved to ./icons/');
