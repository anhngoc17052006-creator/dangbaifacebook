# PostFlow Web — HTML + Python

Bản web hóa từ engine `chayapp.py` gốc. Giao diện chạy bằng HTML/CSS/JavaScript; Python + FastAPI quản lý dữ liệu, Chromium/Playwright đăng nhập và thao tác đăng bài Facebook.

## Chức năng đã nối với backend

- Đăng nhập Facebook bằng cửa sổ Chromium riêng, người dùng tự nhập thông tin trên trang Facebook. PostFlow không hỏi hoặc lưu mật khẩu.
- Đăng bài chữ và tối đa 20 ảnh JPG/PNG/WEBP lên nhiều nhóm Facebook.
- Quản lý và lưu danh sách nhóm; chặn job trùng theo nội dung + ảnh + nhóm.
- Hàng đợi đăng bài, khoảng nghỉ giữa các nhóm, dừng sau bài đang xử lý.
- Theo dõi trạng thái, ghi chú, thời điểm, liên kết bài và thư mục ảnh bằng chứng.
- Xác nhận thủ công kết quả cần đối chiếu; chỉ cho thử lại khi lỗi xảy ra trước khi nhấn Đăng.
- Nhật ký hoạt động, bản nháp, thư viện bài viết, lịch hẹn đăng.
- Lịch được xử lý bởi Python server khi máy tính và server vẫn đang chạy.
- Tối đa 500 MB/ảnh; giới hạn 20 ảnh/bài.

## Cài đặt trên Windows 10/11

1. Cài Python 3.10 trở lên và bật tùy chọn **Add Python to PATH**.
2. Giải nén toàn bộ thư mục `PostFlow_Web` vào máy tính.
3. Nhấp đúp `start.bat`. Lần đầu sẽ tạo môi trường ảo, cài thư viện và Chromium; cần Internet.
4. Trình duyệt mở tại `http://127.0.0.1:8000`.
5. Vào **Đăng nhập Facebook**. Đăng nhập trực tiếp trong cửa sổ Chromium do Playwright mở, sau đó quay lại website bấm **Tôi đã đăng nhập**.
6. Vào **Soạn & đăng bài**, nhập nội dung, tải ảnh, dán link nhóm (mỗi dòng một nhóm), rồi bấm **Đăng bài**.

## Dùng website trên điện thoại cùng Wi-Fi

- Chạy `start_lan.bat` trên máy tính đã cài đặt xong.
- Tìm địa chỉ IPv4 LAN của máy tính bằng lệnh `ipconfig` (ví dụ `192.168.1.20`).
- Trên điện thoại cùng mạng Wi-Fi, mở `http://192.168.1.20:8000`.
- Nếu Windows Firewall hỏi, chỉ cho phép trên **Private network**.
- Chỉ dùng trên mạng đáng tin cậy. Chế độ LAN hiện không có đăng nhập quản trị riêng; bất kỳ thiết bị nào truy cập được cổng này đều có thể điều khiển PostFlow trên máy tính. **Không mở cổng router/port-forward ra Internet, không đưa lên hosting công cộng.**
- Chromium và Python phải chạy trên máy tính; điện thoại chỉ là giao diện điều khiển. Để đăng bài/lịch chạy, máy tính không được tắt hoặc sleep.

## Dữ liệu

Mặc định engine gốc lưu dữ liệu ở thư mục `FB_Auto_Post_V4` trong thư mục người dùng Windows. Có thể đổi bằng biến môi trường `FB_POSTER_HOME` trước khi chạy server. Thư mục này chứa SQLite, hồ sơ Chromium, ảnh upload và bằng chứng đăng bài. Không xóa `trinh_duyet` nếu muốn giữ phiên đăng nhập.

## Lưu ý quan trọng

- Facebook có thể thay đổi giao diện, giới hạn đăng nhóm, yêu cầu checkpoint/xác minh hoặc phê duyệt quản trị viên. Không thể bảo đảm mọi nhóm đều nhận bài.
- Chỉ đăng vào nhóm bạn có quyền đăng và tuân thủ quy định Facebook cũng như nội quy nhóm. Tránh spam và đăng quá dày.
- Trạng thái `Đã nhấn Đăng · Cần đối chiếu` nghĩa là đã có thao tác gửi nhưng chưa xác minh chắc chắn. Không bấm thử lại cho nhóm đó; hãy kiểm tra trực tiếp trên Facebook.
- Không đóng server hoặc tắt máy trong khi đăng. Lịch hẹn chỉ chạy khi Python server đang hoạt động.
- Đây là ứng dụng local-first, không phải website host tĩnh trên GitHub Pages. GitHub Pages không thể chạy Python/Chromium.

## Chạy thủ công

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
python server.py
```

Mặc định server chỉ lắng nghe `127.0.0.1:8000`.
