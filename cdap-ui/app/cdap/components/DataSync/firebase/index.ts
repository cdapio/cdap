import { initializeApp, firestore } from 'firebase';
import 'firebase/firestore';

const config = {};
initializeApp(config);

const db = firestore();

export { db };
