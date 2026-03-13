"""
AIOS Kernel Launcher para Beka MKT
Inicia o kernel AIOS como subprocess e monitora saude.
"""
import subprocess
import sys
import time
import os
import signal
import requests

KERNEL_DIR = os.path.join(os.path.dirname(__file__), "kernel")
KERNEL_PORT = 8000
KERNEL_HOST = "0.0.0.0"
HEALTH_URL = f"http://localhost:{KERNEL_PORT}/core/status"

_kernel_process = None


def start_kernel(port=KERNEL_PORT, host=KERNEL_HOST):
    """Inicia o AIOS kernel como subprocess."""
    global _kernel_process

    if _kernel_process and _kernel_process.poll() is None:
        print("[AIOS] Kernel ja esta rodando (PID: {})".format(_kernel_process.pid))
        return _kernel_process

    env = os.environ.copy()
    env["PYTHONPATH"] = KERNEL_DIR + os.pathsep + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, "-m", "uvicorn",
        "runtime.launch:app",
        "--host", host,
        "--port", str(port),
        "--log-level", "info"
    ]

    print(f"[AIOS] Iniciando kernel na porta {port}...")
    _kernel_process = subprocess.Popen(
        cmd,
        cwd=KERNEL_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Aguardar kernel ficar pronto
    for i in range(30):
        time.sleep(2)
        if _kernel_process.poll() is not None:
            output = _kernel_process.stdout.read() if _kernel_process.stdout else ""
            print(f"[AIOS] Kernel encerrou prematuramente:\n{output[:500]}")
            return None
        try:
            r = requests.get(HEALTH_URL, timeout=3)
            if r.status_code == 200:
                print(f"[AIOS] Kernel online! PID: {_kernel_process.pid}")
                return _kernel_process
        except requests.ConnectionError:
            pass

    print("[AIOS] Timeout aguardando kernel ficar pronto")
    stop_kernel()
    return None


def stop_kernel():
    """Para o kernel AIOS."""
    global _kernel_process
    if _kernel_process and _kernel_process.poll() is None:
        print(f"[AIOS] Parando kernel (PID: {_kernel_process.pid})...")
        _kernel_process.terminate()
        try:
            _kernel_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _kernel_process.kill()
        print("[AIOS] Kernel parado.")
    _kernel_process = None


def is_kernel_alive():
    """Verifica se o kernel AIOS esta respondendo."""
    try:
        r = requests.get(HEALTH_URL, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def get_kernel_status():
    """Retorna status detalhado do kernel."""
    try:
        r = requests.get(HEALTH_URL, timeout=5)
        if r.status_code == 200:
            return {"online": True, "data": r.json()}
    except Exception:
        pass
    return {"online": False, "data": None}


if __name__ == "__main__":
    proc = start_kernel()
    if proc:
        print("[AIOS] Kernel rodando. Pressione Ctrl+C para parar.")
        try:
            while proc.poll() is None:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_kernel()
    else:
        print("[AIOS] Falha ao iniciar kernel.")
        sys.exit(1)
