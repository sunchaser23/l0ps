CREATE TABLE waves_blocks (
    height INTEGER NOT NULL,
    generator TEXT,
    fees INTEGER,
    effectivebalance INTEGER,
    txs INTEGER,
    timestamp INTEGER,
    PRIMARY KEY (height)
);

CREATE TABLE waves_leases (
    tx_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    txtype TEXT,
    address TEXT NOT NULL,
    start INTEGER NOT NULL,
    leasedate INTEGER NOT NULL,
    endleasedate INTEGER,
    end INTEGER,
    amount INTEGER,
    PRIMARY KEY (lease_id, tx_id)
);

CREATE TABLE waves_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    startblock INTEGER NOT NULL,
    endblock INTEGER NOT NULL,
    minedblocks INTEGER NOT NULL,
    summary TEXT,
    paymentlock TEXT,
    timestamp TEXT NOT NULL
);

CREATE TABLE waves_paymentdetails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_id INTEGER NOT NULL,
    address TEXT NOT NULL,
    status TEXT NOT NULL,
    token TEXT NOT NULL,
    token_id TEXT NOT NULL,
    amount INTEGER NOT NULL
);

ALTER TABLE waves_blocks ADD COLUMN tx16calls int;
UPDATE waves_blocks SET tx16calls = 0;
