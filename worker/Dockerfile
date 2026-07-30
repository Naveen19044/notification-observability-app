FROM node:20-slim

WORKDIR /app

COPY package.json .
RUN npm install

COPY worker.js .

EXPOSE 5002

CMD ["node", "worker.js"]
