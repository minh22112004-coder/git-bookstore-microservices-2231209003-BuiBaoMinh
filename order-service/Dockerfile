FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev || npm install --omit=dev
COPY . .
EXPOSE 8003
CMD ["node", "index.js"]
