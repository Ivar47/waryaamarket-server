FROM node:20-alpine

# Security: run as non-root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Install deps first (cached layer)
COPY package.json ./
RUN npm install --omit=dev

# Copy source
COPY server.js ./

# Switch to non-root user
USER appuser

EXPOSE 8080

CMD ["node", "server.js"]
