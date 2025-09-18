#!/bin/bash

# Script để chạy ứng dụng BTC Dominance Crawler
# Sử dụng: ./run.sh [start|stop|restart|status]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="$SCRIPT_DIR/.venv"
MAIN_SCRIPT="$SCRIPT_DIR/src/main.py"
PID_FILE="$SCRIPT_DIR/crawler.pid"

# Colors cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Hàm để log
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Kiểm tra virtual environment
check_venv() {
    if [ ! -d "$VENV_PATH" ]; then
        error "Virtual environment không tồn tại tại $VENV_PATH"
        error "Vui lòng chạy: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
}

# Kiểm tra main script
check_main_script() {
    if [ ! -f "$MAIN_SCRIPT" ]; then
        error "Main script không tồn tại tại $MAIN_SCRIPT"
        exit 1
    fi
}

# Hàm start
start() {
    log "Bắt đầu ứng dụng BTC Dominance Crawler..."

    # Kiểm tra các điều kiện cần thiết
    check_venv
    check_main_script

    # Kiểm tra xem ứng dụng đã chạy chưa
    if [ -f "$PID_FILE" ]; then
        if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            warning "Ứng dụng đã đang chạy với PID $(cat "$PID_FILE")"
            return 1
        else
            warning "Tìm thấy file PID cũ, xóa nó..."
            rm -f "$PID_FILE"
        fi
    fi

    # Activate virtual environment và chạy ứng dụng
    cd "$SCRIPT_DIR"
    source "$VENV_PATH/bin/activate"

    # Chạy ứng dụng trong background
    nohup python "$MAIN_SCRIPT" > crawler.log 2>&1 &
    echo $! > "$PID_FILE"

    success "Ứng dụng đã được khởi động với PID $(cat "$PID_FILE")"
    log "Log file: $SCRIPT_DIR/crawler.log"
}

# Hàm stop
stop() {
    log "Dừng ứng dụng BTC Dominance Crawler..."

    if [ ! -f "$PID_FILE" ]; then
        warning "Không tìm thấy file PID. Ứng dụng có thể chưa chạy."
        return 1
    fi

    PID=$(cat "$PID_FILE")

    if kill -0 "$PID" 2>/dev/null; then
        log "Đang dừng ứng dụng với PID $PID..."
        kill "$PID"

        # Đợi ứng dụng dừng
        for i in {1..10}; do
            if ! kill -0 "$PID" 2>/dev/null; then
                success "Ứng dụng đã được dừng thành công"
                rm -f "$PID_FILE"
                return 0
            fi
            sleep 1
        done

        # Force kill nếu cần
        warning "Ứng dụng không dừng trong 10 giây, force kill..."
        kill -9 "$PID" 2>/dev/null
        success "Ứng dụng đã được force kill"
        rm -f "$PID_FILE"
    else
        warning "Ứng dụng với PID $PID không chạy"
        rm -f "$PID_FILE"
    fi
}

# Hàm restart
restart() {
    log "Restart ứng dụng BTC Dominance Crawler..."
    stop
    sleep 2
    start
}

# Hàm status
status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            success "Ứng dụng đang chạy với PID $PID"
            return 0
        else
            warning "File PID tồn tại nhưng ứng dụng không chạy (PID: $PID)"
            return 1
        fi
    else
        warning "Ứng dụng không chạy (không tìm thấy file PID)"
        return 1
    fi
}

# Hàm show log
show_log() {
    if [ -f "$SCRIPT_DIR/crawler.log" ]; then
        log "Hiển thị log file (nhấn Ctrl+C để thoát):"
        tail -f "$SCRIPT_DIR/crawler.log"
    else
        warning "Không tìm thấy file log: $SCRIPT_DIR/crawler.log"
    fi
}

# Main logic
case "${1:-help}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    log)
        show_log
        ;;
    help|*)
        echo "Sử dụng: $0 {start|stop|restart|status|log|help}"
        echo ""
        echo "Các lệnh:"
        echo "  start   - Khởi động ứng dụng"
        echo "  stop    - Dừng ứng dụng"
        echo "  restart - Restart ứng dụng"
        echo "  status  - Kiểm tra trạng thái ứng dụng"
        echo "  log     - Hiển thị log file"
        echo "  help    - Hiển thị trợ giúp này"
        echo ""
        echo "Ví dụ:"
        echo "  ./run.sh start    # Khởi động ứng dụng"
        echo "  ./run.sh stop     # Dừng ứng dụng"
        echo "  ./run.sh status   # Kiểm tra trạng thái"
        echo "  ./run.sh log      # Xem log"
        ;;
esac