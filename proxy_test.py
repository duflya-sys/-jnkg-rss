# proxy_test.py - 代理测试
import requests
import os

def test_proxy():
    """测试代理是否可用"""
    proxy_url = "http://113.121.39.222:9999"
    test_urls = [
        "https://www.baidu.com",
        "https://dzzb.jnkgjtdzzbgs.com",
        "https://api.ipify.org?format=json"  # 查看当前IP
    ]
    
    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }
    
    print(f"🔍 测试代理: {proxy_url}")
    
    for url in test_urls:
        try:
            print(f"\n测试URL: {url}")
            
            # 测试直接连接
            print("1. 直接连接测试...")
            try:
                response = requests.get(url, timeout=10)
                print(f"   直接连接: ✅ 成功 (状态码: {response.status_code})")
            except Exception as e:
                print(f"   直接连接: ❌ 失败 ({e})")
            
            # 测试代理连接
            print("2. 代理连接测试...")
            try:
                response = requests.get(url, proxies=proxies, timeout=10)
                print(f"   代理连接: ✅ 成功 (状态码: {response.status_code})")
                
                if "ipify" in url:
                    print(f"   当前IP: {response.json()}")
                    
            except Exception as e:
                print(f"   代理连接: ❌ 失败 ({e})")
                
        except Exception as e:
            print(f"测试异常: {e}")
    
    print("\n" + "="*60)
    print("📊 代理测试完成")

if __name__ == "__main__":
    test_proxy()
