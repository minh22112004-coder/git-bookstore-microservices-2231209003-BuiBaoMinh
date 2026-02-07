import express from 'express';
import db from './db.js';

const app = express();
app.use(express.json());

// List products
app.get('/', async (_req, res) => {
  try {
    const r = await db.query('SELECT * FROM books ORDER BY id');
    res.json(r.rows);
  } catch (err) {
    console.error('List products error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get product by id
app.get('/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const r = await db.query('SELECT * FROM books WHERE id=$1', [id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Product not found' });
    res.json(r.rows[0]);
  } catch (err) {
    console.error('Get product error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Create product
app.post('/', async (req, res) => {
  try {
    const { title, author, price, stock = 100 } = req.body;
    if (!title || !author) return res.status(400).json({ error: 'title and author required' });
    const r = await db.query(
      'INSERT INTO books (title, author, price, stock) VALUES ($1,$2,$3,$4) RETURNING *',
      [title, author, price ?? 0, stock]
    );
    res.status(201).json(r.rows[0]);
  } catch (err) {
    console.error('Create product error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update product
app.put('/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { title, author, price, stock } = req.body;
    const r = await db.query(
      'UPDATE books SET title = COALESCE($1,title), author = COALESCE($2,author), price = COALESCE($3,price), stock = COALESCE($4,stock) WHERE id=$5 RETURNING *',
      [title ?? null, author ?? null, price ?? null, stock ?? null, id]
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Product not found' });
    res.json(r.rows[0]);
  } catch (err) {
    console.error('Update product error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Delete product
app.delete('/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const r = await db.query('DELETE FROM books WHERE id=$1 RETURNING id', [id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Product not found' });
    res.json({ message: 'Deleted', id });
  } catch (err) {
    console.error('Delete product error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = 8002;
app.listen(PORT, () => console.log(`Product Service running on ${PORT}`));
