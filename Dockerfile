FROM node:alpine3.21 AS build 
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci 
COPY . . 
RUN npm run build 


FROM node:alpine3.21 AS runtime
WORKDIR /app 
COPY --from=build /app/dist ./dist
COPY package.json package-lock.json ./
# RUN npm ci --omit=dev
RUN npm ci

CMD ["node", "dist/index.js"]
