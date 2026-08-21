CREATE SCHEMA IF NOT EXISTS kebm;

CREATE TYPE withdrawal_status AS ENUM ('pending', 'succeeded', 'failed');

CREATE TABLE kebm.withdrawal_data
(
    id                BIGSERIAL              NOT NULL,
    party_id          UUID,
    withdrawal_id     CHARACTER VARYING      NOT NULL,
    wallet_id         CHARACTER VARYING      NOT NULL,
    created_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    amount            BIGINT                 NOT NULL,
    currency_code     CHARACTER VARYING      NOT NULL,
    withdrawal_status kebm.withdrawal_status NOT NULL,
    provider_id       INT,
    terminal_id       INT,
    CONSTRAINT withdrawal_data_pkey PRIMARY KEY (id),
    CONSTRAINT withdrawal_data_ukey UNIQUE (withdrawal_id)
);

CREATE INDEX withdrawal_cleanup_idx ON kebm.withdrawal_data (created_at, id);

CREATE TYPE invoice_payment_status AS enum ('pending', 'processed', 'captured', 'cancelled', 'failed', 'refunded');

CREATE TABLE kebm.invoice_payment_data
(
    id             BIGSERIAL                   NOT NULL,
    invoice_id     CHARACTER VARYING           NOT NULL,
    payment_id     CHARACTER VARYING           NOT NULL,
    party_id       UUID,
    shop_id        CHARACTER VARYING           NOT NULL,
    created_at     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    currency_code  CHARACTER VARYING           NOT NULL,
    amount         BIGINT                      NOT NULL,
    payment_status kebm.invoice_payment_status NOT NULL,
    provider_id    INT,
    terminal_id    INT,
    CONSTRAINT invoice_payment_data_pkey PRIMARY KEY (id),
    CONSTRAINT invoice_payment_data_ukey UNIQUE (invoice_id, payment_id)
);

CREATE INDEX invoice_payment_cleanup_idx ON kebm.invoice_payment_data (created_at, id);

