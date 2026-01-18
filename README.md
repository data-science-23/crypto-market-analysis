## Hướng dẫn chạy project

### 1. Tải model
- Truy cập Google Drive:  
  [models](https://drive.google.com/drive/folders/1JpzlnzvWyfREbFRWKcrO7Bw6CB-kScj1?usp=sharing)
- Tải toàn bộ **models**
- Copy và bỏ vào thư mục `models/` của project

---

### 2. Vector database
- Truy cập Google Drive:  
  [chroma_db](https://drive.google.com/drive/folders/1qgHoIFFlCXkD4-zhhGxFOMxr37m5Fiuz?usp=sharing)
- Tải một trong các file zip (tùy theo lượng data muốn sử dụng)
- Giải nén và để thư mục `chroma_db/` vào project

---

### 3. Tạo Virtual Environment

- Nếu là Window: 
```bash
python -m venv .venv
.venv\Scripts\activate
```

- Nếu là macOS / Linux (bash, zsh)
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Khởi tạo các biến môi trường
- Tạo file .env và điền thông tin theo mẫu ở file .env.example

### 5. Chạy Backend
```bash
pip install -r requirements.txt
python backend/main.py
```
### 6. Chạy Frontend

Mở terminal khác:
```bash
npm install
npm run dev
```
Ảnh kết quả training nằm trong thư mục evaluation result