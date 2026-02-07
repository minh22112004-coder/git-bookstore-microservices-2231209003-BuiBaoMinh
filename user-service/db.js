import mongoose from 'mongoose';
import dotenv from 'dotenv';
dotenv.config();

const mongoUrl = process.env.MONGO_URL || 'mongodb://user-db:27017/bookstore';

mongoose.connect(mongoUrl, { dbName: process.env.MONGO_DB || 'bookstore' })
  .then(() => console.log('Connected to MongoDB for user-service'))
  .catch(err => console.error('Mongo connection error:', err.message));

const userSchema = new mongoose.Schema({
  username: { type: String, unique: true, required: true },
  password: { type: String, required: true },
}, { timestamps: true });

const User = mongoose.models.User || mongoose.model('User', userSchema);
export default User;
