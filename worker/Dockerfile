FROM node:21-alpine3.18

WORKDIR /src

COPY . .

RUN npm install
RUN npx tsc

EXPOSE 3000

CMD ["node","dist/worker.js"]