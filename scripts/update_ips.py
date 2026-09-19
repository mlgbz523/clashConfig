import os
import sys
import time
import csv
import random
import ipaddress
import asyncio
import aiohttp
import subprocess
from datetime import datetime

# ================= 核心配置区域 =================
# 当前脚本所在目录 (clashConfig/scripts)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 仓库根目录 (clashConfig)，配置文件 ips_*.txt 写入此处
GIT_REPO_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "result.csv")
CFST_EXE_PATH = os.path.join(SCRIPT_DIR, "cfst.exe" if sys.platform == 'win32' else "cfst")

# ★ 4 个轮转文件列表 (保存在根目录) ★
ROTATION_FILES = ["ips_1.txt", "ips_2.txt", "ips_3.txt", "ips_4.txt"]

# ★ Cloudflare HTTPS 端口池与随机端口选择 ★
CF_HTTPS_PORTS = [443, 8443, 2053, 2083, 2087, 2096]
CURRENT_PORT = random.choice(CF_HTTPS_PORTS)

# --- 引擎 1 参数 (CT1) ---
CFST_SEARCH_LIMIT = 20    
CFST_TOP_N = 20           

# --- 引擎 2 参数 (CT2 - 延迟优选) ---
TARGET_COUNTRIES = ["SG", "US", "DE"]  
CFS_TOP_N = 10            
MAX_WORKERS = 150         
MAX_LATENCY = 220         
MIN_SPEED = 1.0           
# ==========================================

def get_country_code(iata):
    mapping = {
        "HKG": "HK", "TPE": "TW", "NRT": "JP", "KIX": "JP",
        "ICN": "KR", "SIN": "SG", "BKK": "TH", "KUL": "MY",
        "SJC": "US", "LAX": "US", "SFO": "US", "SEA": "US",
        "ORD": "US", "DFW": "US", "MIA": "US", "EWR": "US",
        "FRA": "DE", "MUC": "DE", "BER": "DE",
        "LHR": "UK", "CDG": "FR", "AMS": "NL"
    }
    return mapping.get(iata, iata)

CF_IPV4_CIDRS = [
    "173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
    "141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20",
    "197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/12",
    "172.64.0.0/13", "131.0.72.0/22"
]

# ================= 引擎 1：CT1 =================
async def run_cfst(target_port):
    print(f"\n[引擎1: CT1] 正在开启独立窗口执行 (测试端口: {target_port})...")
    
    if sys.platform == 'win32':
        bat_path = os.path.join(SCRIPT_DIR, "run_ct1_temp.bat")
        with open(bat_path, "w", encoding="utf-8") as f:
            f.write(f'@echo off\n"{CFST_EXE_PATH}" -o "{CSV_FILE_PATH}" -dn {CFST_SEARCH_LIMIT} -tl {MAX_LATENCY} -sl {MIN_SPEED} -tp {target_port} < nul\nexit\n')
        
        await asyncio.to_thread(
            subprocess.run, 
            [bat_path], 
            cwd=SCRIPT_DIR, 
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        if os.path.exists(bat_path):
            try: os.remove(bat_path)
            except: pass
    else:
        cmd = [CFST_EXE_PATH, "-o", CSV_FILE_PATH, "-dn", str(CFST_SEARCH_LIMIT), "-tl", str(MAX_LATENCY), "-sl", str(MIN_SPEED), "-tp", str(target_port)]
        await asyncio.to_thread(subprocess.run, cmd, cwd=SCRIPT_DIR, input=b'\n')
    
    raw_nodes = []
    if os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ip = row.get('IP 地址', '').strip()
                iata = row.get('地区码', 'CF').strip().upper()
                try: speed = float(row.get('下载速度(MB/s)', '0'))
                except ValueError: speed = 0.0
                
                if ip and speed >= MIN_SPEED:
                    country = get_country_code(iata)
                    raw_nodes.append({
                        'ip': ip, 'port': target_port, 'country': country, 
                        'speed': speed, 'source': 'CT1'
                    })
                    
    raw_nodes.sort(key=lambda x: x['speed'], reverse=True)
    top_nodes = raw_nodes[:CFST_TOP_N]
    print(f"[引擎1: CT1] 独立窗口运行结束！提取了 {len(top_nodes)} 个节点。")
    return top_nodes

# ================= 引擎 2：CT2 =================
def generate_ipv4s():
    ip_list = []
    for cidr in CF_IPV4_CIDRS:
        try:
            network = ipaddress.ip_network(cidr, strict=False)
            for subnet in network.subnets(new_prefix=24):
                if subnet.num_addresses > 12:
                    hosts = list(subnet.hosts())
                    if hosts:
                        sampled_ips = random.sample(hosts, min(1, len(hosts)))
                        ip_list.extend([str(ip) for ip in sampled_ips])
        except Exception:
            continue
    random.shuffle(ip_list)
    return ip_list

async def get_iata_code_async(session: aiohttp.ClientSession, ip: str, timeout: int = 2):
    url = f"http://{ip}/cdn-cgi/trace"
    headers = {"User-Agent": "Mozilla/5.0", "Host": "speed.cloudflare.com"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200:
                text = await resp.text()
                for line in text.strip().split('\n'):
                    if line.startswith('colo='):
                        return line.split('=', 1)[1].strip().upper()
    except Exception:
        pass
    return None

async def run_cfs(target_port):
    print(f"\n=== [主窗口] 启动引擎2 (CT2) - 测试端口: {target_port} ===")
    ip_list = generate_ipv4s()
    total_ips = len(ip_list)
    print(f"[CT2] 开始 IPv4 扫描，并发: {MAX_WORKERS}，共 {total_ips} 个 IP")
    
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS, force_close=True)
    candidates = {cc: [] for cc in TARGET_COUNTRIES}
    completed = 0

    async def check_ip_latency(session, ip):
        nonlocal completed
        async with semaphore:
            try:
                start = time.monotonic()
                try:
                    r, w = await asyncio.wait_for(asyncio.open_connection(ip, target_port), timeout=MAX_LATENCY/1000.0)
                    w.close(); await w.wait_closed()
                    latency = (time.monotonic() - start) * 1000
                except Exception:
                    return

                if latency > MAX_LATENCY:
                    return

                iata = await get_iata_code_async(session, ip, 2)
                if not iata: return
                country = get_country_code(iata)
                
                if country in TARGET_COUNTRIES:
                    candidates[country].append({
                        'ip': ip, 'port': target_port, 'country': country, 'latency': latency
                    })
            finally:
                completed += 1
                if completed % 200 == 0 or completed == total_ips:
                    found_str = " ".join([f"{cc}:{len(candidates[cc])}" for cc in TARGET_COUNTRIES])
                    print(f"\r[CT2 扫描进度] {completed} / {total_ips} [候选池: {found_str}]...   ", end="", flush=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(check_ip_latency(session, ip)) for ip in ip_list]
        await asyncio.gather(*tasks)
    
    print(f"\n[CT2] 扫描阶段结束！按延迟抽取优选节点...")
    final_cfs = []

    for cc in TARGET_COUNTRIES:
        c_nodes = candidates.get(cc, [])
        c_nodes.sort(key=lambda x: x['latency'])
        
        top_cc_nodes = c_nodes[:CFS_TOP_N]
        for node in top_cc_nodes:
            node['source'] = 'CT2'
            node['speed'] = round(max(10.0, 300 - node['latency']), 2)
            final_cfs.append(node)
            print(f"   [+] CT2 选中: {node['ip']}:{target_port} | 地区: {cc} | 延迟: {node['latency']:.1f}ms")
                
    print(f"[引擎2: CT2] 提取完成！成功获取了 {len(final_cfs)} 个节点。")
    return final_cfs

# ================= 4 文件轮转算法 =================
def select_target_file(repo_path):
    empty_files = []
    existing_files_with_mtime = []

    for filename in ROTATION_FILES:
        full_path = os.path.join(repo_path, filename)
        if not os.path.exists(full_path) or os.path.getsize(full_path) == 0:
            empty_files.append(filename)
        else:
            existing_files_with_mtime.append((filename, os.path.getmtime(full_path)))

    if empty_files:
        chosen = empty_files[0]
        print(f"[*] 写入策略：发现空闲槽位 -> 写入 【{chosen}】")
        return chosen
    
    existing_files_with_mtime.sort(key=lambda x: x[1])
    oldest_file = existing_files_with_mtime[0][0]
    print(f"[*] 写入策略：4个槽位均已满，轮转覆盖最旧的文件 -> 【{oldest_file}】")
    return oldest_file

# ================= 合并、写入与提交 =================
def process_and_push(cfst_nodes, cfs_nodes):
    all_nodes = cfst_nodes + cfs_nodes
    if not all_nodes:
        print("\n[!] 警告：未找到符合要求的节点。")
        return

    unique_nodes = {}
    for node in all_nodes:
        ip = node['ip']
        if ip not in unique_nodes or node['speed'] > unique_nodes[ip]['speed']:
            unique_nodes[ip] = node

    grouped_final = {}
    for node in unique_nodes.values():
        cc = node['country']
        if cc not in grouped_final:
            grouped_final[cc] = []
        grouped_final[cc].append(node)

    print("\n=== 双引擎去重合并完成 ===")
    formatted_ips = []
    for cc, nodes in grouped_final.items():
        nodes.sort(key=lambda x: x['speed'], reverse=True)
        print(f"\n [+] 地区: {cc} | 节点数: {len(nodes)}")
        for n in nodes:
            line = f"{n['ip']}:{n['port']}#{n['country']}-[{n['source']}]"
            formatted_ips.append(line)
            print(f"    写入 -> {line}")

    # 将节点文件写入根目录下的目标轮转文件
    target_filename = select_target_file(GIT_REPO_PATH)
    txt_full_path = os.path.join(GIT_REPO_PATH, target_filename)

    with open(txt_full_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(formatted_ips))
    
    print(f"\n[*] 成功写入 {len(formatted_ips)} 个节点到根目录 {target_filename}")

    # 切回仓库根目录执行 Git 提交
    print("[*] 准备推送至 GitHub...")
    os.chdir(GIT_REPO_PATH)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        subprocess.run(["git", "add", "."], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-m", f"Auto-update: {target_filename} ({timestamp})"], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["git", "push"], check=True, stdout=subprocess.DEVNULL)
        print("[√] 成功推送到 GitHub 远端仓库！")
    except subprocess.CalledProcessError:
        print("[!] Git 推送跳过（内容未发生变动）。")
    finally:
        os.chdir(SCRIPT_DIR)

async def main():
    print(f"=== 启动双引擎 (本次自动轮换选用端口: {CURRENT_PORT}) ===")
    task1 = asyncio.create_task(run_cfst(CURRENT_PORT))
    task2 = asyncio.create_task(run_cfs(CURRENT_PORT))
    
    cfst_nodes, cfs_nodes = await asyncio.gather(task1, task2)
    process_and_push(cfst_nodes, cfs_nodes)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())