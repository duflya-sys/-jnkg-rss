import os
import sys
import pandas as pd
from datetime import datetime
import time

# 将当前目录加入路径，确保能导入自定义模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入自定义模块
try:
    from spider_core import JnkgBiddingSpider
    from feishu_writer import FeishuBitableWriter
    from feishu_notifier import FeishuNotifier
except ImportError as e:
    print(f"导入模块失败，请确保相关.py文件在当前目录: {e}")
    sys.exit(1)

def get_feishu_config():
    """从环境变量获取飞书配置（安全）"""
    config = {
        'app_id': os.getenv('FEISHU_APP_ID'),
        'app_secret': os.getenv('FEISHU_APP_SECRET'),
        'app_token': os.getenv('FEISHU_APP_TOKEN'),
        'table_id': os.getenv('FEISHU_TABLE_ID'),
        'webhook_url': os.getenv('FEISHU_WEBHOOK_URL')
    }
    
    # 清理可能的多余字符
    if config['app_token'] and '&' in config['app_token']:
        config['app_token'] = config['app_token'].split('&')[0]
    
    if config['table_id'] and '&' in config['table_id']:
        config['table_id'] = config['table_id'].split('&')[0]
    
    # 检查关键配置是否存在
    missing = [k for k, v in config.items() if not v and k != 'webhook_url']
    if missing:
        print(f"⚠️  警告：以下飞书配置缺失: {missing}")
        return None
    
    print(f"🔧 飞书配置详情:")
    print(f"   App ID: {config['app_id'][:10]}..." if config['app_id'] else "   App ID: 未设置")
    print(f"   App Token: {config['app_token']}")
    print(f"   Table ID: {config['table_id']}")
    print(f"   Webhook URL: {'已设置' if config['webhook_url'] else '未设置'}")
    
    return config
# 在main.py中添加代理测试函数
def test_network_connectivity():
    """测试网络连通性"""
    print("🔍 测试网络连通性...")
    
    # 检查是否在GitHub Actions环境
    is_github = os.getenv('GITHUB_ACTIONS') == 'true'
    print(f"GitHub Actions环境: {is_github}")
    
    if is_github:
        print("🌐 检测到GitHub Actions环境，将启用代理")
        print("代理地址: http://117.69.236.166:8089")
    
    # 导入代理测试
    try:
        from proxy_test import test_proxy
        test_proxy()
    except ImportError:
        print("⚠️  代理测试模块未找到，跳过测试")

# 在run_full_process函数开头添加
def run_full_process(days_limit=10):
    """完整的抓取和上传流程"""
    print("="*60)
    print(f"开始执行晋能控股招标数据抓取任务")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 网络测试
    test_network_connectivity()

def run_full_process(days_limit=10):
    """完整的抓取和上传流程"""
    print("="*60)
    print(f"开始执行晋能控股招标数据抓取任务")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 1. 初始化爬虫并抓取数据
    print("\n🔍 步骤1: 开始抓取招标数据...")
    spider = JnkgBiddingSpider()
    
    # 使用新的多网站搜索方法
    all_data = spider.search_all_websites(days_limit=days_limit)
    
    if not all_data:
        print("本次未抓取到符合条件的数据。任务结束。")
        # 尝试发送空数据通知（如果配置了webhook）
        feishu_config = get_feishu_config()
        if feishu_config and feishu_config.get('webhook_url'):
            try:
                notifier = FeishuNotifier(feishu_config['webhook_url'])
                notifier.send_text("🕷️ 招标数据抓取完成\n\n本次未抓取到符合条件的数据。")
            except Exception as e:
                print(f"发送空数据通知失败: {e}")
        return False, 0, 0, 0
    
    df = pd.DataFrame(all_data)
    print(f"✅ 抓取完成，共获得 {len(df)} 条唯一数据。")
    
    # 2. 上传到飞书多维表格
    print("\n📤 步骤2: 准备上传数据到飞书多维表格...")
    feishu_config = get_feishu_config()
    
    if not feishu_config:
        print("由于飞书配置不全，跳过上传步骤。")
        # 本地保存一份CSV作为备份
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = f"本地备份_晋能控股招标_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"数据已本地备份至: {csv_file}")
        return True, len(df), 0, 0
    
    try:
        # 初始化飞书写入器
        writer = FeishuBitableWriter(
            app_id=feishu_config['app_id'],
            app_secret=feishu_config['app_secret'],
            app_token=feishu_config['app_token'],
            table_id=feishu_config['table_id'],
            debug=True
        )
        
        # 上传数据，使用'项目编号'作为去重依据
        success, fail, duplicate = writer.add_records(df, unique_key_field='项目编号')
        
        print("\n📊 上传结果汇总:")
        print(f"   成功新增: {success} 条")
        print(f"   重复跳过: {duplicate} 条")
        print(f"   添加失败: {fail} 条")
        
        # 3. 本地也保存一份CSV作为备份
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = f"晋能控股招标_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"📁 数据已备份至本地文件: {csv_file}")
        
        # 4. 发送飞书机器人提醒（如果配置了webhook）
        if feishu_config.get('webhook_url'):
            print("\n📨 步骤3: 发送飞书机器人提醒...")
            try:
                notifier = FeishuNotifier(feishu_config['webhook_url'])
                # 使用卡片消息格式
                report = notifier.send_crawler_report_with_card(
                    total_count=len(df),
                    success_count=success,
                    duplicate_count=duplicate,
                    fail_count=fail
                )
                if report and report.get("StatusCode") == 0:
                    print("✅ 飞书机器人卡片提醒发送成功！")
                else:
                    # 如果卡片消息失败，尝试普通文本消息
                    print(f"⚠️  卡片消息发送失败，尝试文本消息...")
                    report = notifier.send_crawler_report(
                        total_count=len(df),
                        success_count=success,
                        duplicate_count=duplicate,
                        fail_count=fail
                    )
                    if report and report.get("StatusCode") == 0:
                        print("✅ 飞书机器人文本提醒发送成功！")
                    else:
                        print(f"⚠️  飞书机器人提醒发送失败，响应: {report}")
            except Exception as e:
                print(f"❌ 发送飞书机器人提醒时出错: {e}")
        else:
            print("\nℹ️  未配置飞书Webhook URL，跳过通知步骤")
        
        return True, len(df), success, duplicate
        
    except Exception as e:
        print(f"❌ 上传到飞书过程中发生错误: {e}")
        # 出错时也保存本地备份
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = f"错误备份_晋能控股招标_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"数据已保存至本地备份文件: {csv_file}")
        
        # 错误时也发送提醒（如果配置了webhook）
        if feishu_config and feishu_config.get('webhook_url'):
            try:
                notifier = FeishuNotifier(feishu_config['webhook_url'])
                error_msg = f"❌ 招标数据抓取任务失败\n\n错误时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n错误详情: {str(e)}"
                notifier.send_text(error_msg)
                print("✅ 错误通知已发送至飞书。")
            except Exception as notify_error:
                print(f"❌ 发送错误通知也失败了: {notify_error}")
            
        return False, len(df), 0, 0

if __name__ == "__main__":
    """
    主入口。
    当直接运行此脚本时，执行一次完整的抓取和上传。
    此脚本也可被 GitHub Actions 或 APScheduler 调用。
    """
    # 执行完整的抓取上传流程（默认查最近10天）
    run_full_process(days_limit=10)
