FROM node:9-alpine

WORKDIR /home/user/escrow

COPY src src/
COPY yarn.lock package.json ./

RUN apk add --no-cache --virtual .build-deps \
      python make g++ \
    && yarn \
    && npm rebuild bcrypt --build-from-source \
    && apk del .build-deps

# RUN yarn run build

EXPOSE 3000

CMD yarn start