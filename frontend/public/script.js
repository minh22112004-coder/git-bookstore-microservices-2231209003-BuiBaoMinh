const api = (path) => (window.API_BASE || '') + path;

function token() {
  return localStorage.getItem('token');
}
function authHeaders() {
  const t = token();
  return t ? { 'Authorization': 'Bearer ' + t } : {};
}
function el(id) { return document.getElementById(id); }

async function fetchJSON(path, options = {}) {
  const res = await fetch(api(path), {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}), ...authHeaders() }
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || 'Request failed');
  return data;
}

function renderProducts(products) {
  const list = el('product-list');
  list.innerHTML = '';
  products.forEach(p => {
    const card = document.createElement('div');
    card.className = 'product-card';
    card.innerHTML = `<h3>#${p.id} ${p.title}</h3>
      <p>by ${p.author}</p>
      <p><b>$${p.price}</b> — stock: ${p.stock}</p>
      <button data-id="${p.id}">Order</button>`;
    card.querySelector('button').onclick = () => {
      el('product-id').value = p.id;
      window.scrollTo({ top: el('create-order').offsetTop, behavior: 'smooth' });
    };
    list.appendChild(card);
  });
}

function renderOrders(orders) {
  const list = el('order-list');
  list.innerHTML = orders.map(o => `<div class="order-card">
    <strong>Order #${o.id}</strong> — product ${o.product_id}, qty ${o.quantity}
    <em>status: ${o.status}</em>
  </div>`).join('');
}

async function refresh() {
  const [products, orders] = await Promise.all([
    fetchJSON('/api/products'),
    fetchJSON('/api/orders').catch(() => [])
  ]);
  renderProducts(products);
  renderOrders(orders);
}

document.addEventListener('DOMContentLoaded', () => {
  // Auth UI
  const showLogin = (show) => el('login-panel').style.display = show ? '' : 'none';
  const updateAuthBar = async () => {
    const t = token();
    if (!t) {
      el('auth-status').textContent = 'Not signed in';
      el('btn-show-login').style.display = '';
      el('btn-logout').style.display = 'none';
      return;
    }
    try {
      const me = await fetchJSON('/api/users/me');
      el('auth-status').textContent = 'Signed in as ' + me.username;
      el('btn-show-login').style.display = 'none';
      el('btn-logout').style.display = '';
      showLogin(false);
    } catch {
      localStorage.removeItem('token');
      updateAuthBar();
    }
  };
  el('btn-show-login').onclick = () => showLogin(true);
  el('btn-logout').onclick = () => { localStorage.removeItem('token'); updateAuthBar(); };

  el('btn-login').onclick = async () => {
    try {
      const data = await fetchJSON('/api/users/login', {
        method: 'POST',
        body: JSON.stringify({ username: el('username').value, password: el('password').value })
      });
      localStorage.setItem('token', data.token);
      el('auth-message').textContent = 'Logged in!';
      updateAuthBar();
    } catch (e) {
      el('auth-message').textContent = e.message;
    }
  };
  el('btn-register').onclick = async () => {
    try {
      const data = await fetchJSON('/api/users/register', {
        method: 'POST',
        body: JSON.stringify({ username: el('username').value, password: el('password').value })
      });
      localStorage.setItem('token', data.token);
      el('auth-message').textContent = 'Registered & logged in!';
      updateAuthBar();
    } catch (e) {
      el('auth-message').textContent = e.message;
    }
  };

  // Create product
  el('create-product-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const body = {
      title: el('title').value,
      author: el('author').value,
      price: Number(el('price').value),
      stock: Number(el('stock').value)
    };
    try {
      await fetchJSON('/api/products', { method: 'POST', body: JSON.stringify(body) });
      el('create-product-status').textContent = 'Created!';
      refresh();
    } catch (err) {
      el('create-product-status').textContent = err.message;
    }
  });

  // Place order
  el('order-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      await fetchJSON('/api/orders', {
        method: 'POST',
        body: JSON.stringify({
          productId: Number(el('product-id').value),
          quantity: Number(el('quantity').value)
        })
      });
      el('order-status').textContent = 'Order placed!';
      refresh();
    } catch (err) {
      el('order-status').textContent = err.message;
    }
  });

  updateAuthBar();
  refresh();
});
