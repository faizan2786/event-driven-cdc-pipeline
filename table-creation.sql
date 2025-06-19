-- create USERS table in postgres database
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    dob DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- create ORDERS table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    status VARCHAR(10) NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id),
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    placed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE -- TRUE if the associated user is deleted
);

-- create a trigger function to update is_deleted field in ORDERS table on USERS deletion
CREATE OR REPLACE FUNCTION update_order_is_deleted()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders
    SET is_deleted = TRUE
    WHERE user_id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- create a trigger to call above function when is_deleted is set to TRUE in USERS table
CREATE TRIGGER user_is_deleted_trigger
AFTER UPDATE OF is_deleted ON users
FOR EACH ROW
WHEN (OLD.is_deleted IS FALSE AND NEW.is_deleted IS TRUE)
EXECUTE FUNCTION update_order_is_deleted();