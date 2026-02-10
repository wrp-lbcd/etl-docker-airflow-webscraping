# 🚀 HomePro Web Scraping to GCS Pipeline (Airflow)
ฝึก Web Scraping ด้วยข้อมูลบนเว็บไซต์ของ [![HomePro](https://img.shields.io/badge/Source-HomePro_Air_Conditioners-orange?style=for-the-badge&logo=homeadvisor)](https://www.homepro.co.th/c/APP01?dcx=d&s=12&size=100&q=%E0%B9%80%E0%B8%84%E0%B8%A3%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B8%87%E0%B8%9B%E0%B8%A3%E0%B8%B1%E0%B8%9A%E0%B8%AD%E0%B8%B2%E0%B8%81%E0%B8%B2%E0%B8%A8&page=1)
โดยจะเซฟเป็นไฟล์ CSV และควบคุมทั้ง Workflow ด้วย Apache Airflow ที่รันบน Docker โดยมีการประมูลผลข้อมูลด้วย Pandas และจัดเก็บลงใน Google Cloud Storage (GCS)

## 📋 รายละเอียดของ (Workflow)
Pipeline ชุดนี้ประกอบด้วย 2 งานหลัก (Tasks):
1. scraping_data:
- ดึงข้อมูลจากหน้าเว็บ HomePro จำนวน 4 หน้า
- เก็บข้อมูล: SKU, Brand, Title, Discounted Price และ Full Price
- บันทึกเป็นไฟล์ .csv ลงในเครื่อง (Local Path: /tmp/)

2. load_to_gcs:
- อัปโหลดไฟล์ CSV ที่ได้ขึ้นไปยัง Google Cloud Storage Bucket ที่กำหนด

## 🛠️ Stack ที่ใช้
- Orchestration: Apache Airflow
- Language: Python
- Libraries: requests, BeautifulSoup4 (bs4), pandas
- Cloud Platform: Google Cloud Platform (GCS)

## ⚙️ การตั้งค่าก่อนใช้งาน
1. Google Cloud Platform
- สร้าง GCS Bucket (ในโค้ดตัวอย่างใช้ชื่อ bucket-th)
- สร้าง Service Account ที่มีสิทธิ์ Storage Object Admin
- ดาวน์โหลด JSON Key และนำไปตั้งค่าใน Airflow

2. Airflow Connections
- ไปที่ Airflow Web UI > Admin > Connections

2.1 สร้าง Connection ใหม่
- Conn Id: google_cloud_default (ใช้ชื่ออะไรก้ได้)
- Conn Type: Google Cloud
- Keyfile JSON: (วางเนื้อหาจากไฟล์ JSON Key ที่ได้จาก GCP)

## 📂 Dataset Columns Description
ไฟล์จะถูกเก็บใน GCS: data/scraping.csv 

| Column Name         | Description                                  |
|---------------------|----------------------------------------------|
| No                  | ลำดับรายการสินค้า                             |
| SKU                 | รหัสสินค้าจาก HomePro                        |
| Brand               | ยี่ห้อสินค้า                                 |
| Title               | ชื่อรุ่น / รายละเอียดสินค้า                  |
| Discounted_Price    | ราคาขายปัจจุบัน (หน่วย: บาท)                 |
| Full_Price          | ราคาเต็มก่อนลด (หน่วย: บาท)                  |


