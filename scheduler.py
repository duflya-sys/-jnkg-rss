# scheduler.py
import sys
import os
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# 将当前目录加入路径，确保能导入自定义模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入我们写好的主流程和通知器
from main import run_full_process
from feishu_notifier import FeishuNotifier

# --- 定时任务函数 ---
def scheduled_crawler_job():
    """
    这个函数将被定时调用。
    它封装了完整的抓取和上传流程。
    """
    job_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"[{job_time}] APScheduler 触发定时任务！")
    print(f"触发时间：每周三、周五 18:00")
    print('='*60)

    try:
        # 从环境变量获取webhook_url
        webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
        
        if not webhook_url:
            print("⚠️  未配置飞书Webhook URL，跳过通知")
        
        # 调用主流程，执行真正的抓取和上传
        # 参数 days_limit=5 表示抓取最近5天的数据
        result = run_full_process(days_limit=5)
        
        # 解析返回结果
        # run_full_process 返回 (success, total_count, success_count, duplicate_count)
        if result:
            success, total, added, duplicate = result
            fail = total - added - duplicate
            
            # 只有配置了webhook_url才发送通知
            if webhook_url:
                # 发送飞书机器人通知
                notifier = FeishuNotifier(webhook_url)
                # 使用卡片消息格式
                report = notifier.send_crawler_report_with_card(total, added, duplicate, fail)
                
                if report and report.get("StatusCode") == 0:
                    print("✅ 抓取报告已成功发送至飞书。")
                else:
                    print(f"⚠️  飞书通知发送可能失败，响应: {report}")
            else:
                print("ℹ️  未配置Webhook URL，跳过飞书通知")
        else:
            print("⚠️  本次抓取未返回有效结果，跳过飞书通知。")
            
    except Exception as e:
        print(f"❌ 定时任务执行过程中出现异常: {e}")
        
        # 即使出错也尝试发送错误通知（如果配置了webhook）
        try:
            webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
            if webhook_url:
                notifier = FeishuNotifier(webhook_url)
                error_msg = f"❌ 招标数据抓取任务失败\n\n错误时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n错误详情: {str(e)}"
                notifier.send_text(error_msg)
                print("✅ 错误通知已发送至飞书。")
        except Exception as notify_error:
            print(f"❌ 发送错误通知也失败了: {notify_error}")

    print(f"[{datetime.now()}] 本次定时任务执行完毕。")
    print('='*60)

# --- 主程序：设置并启动定时器 ---
if __name__ == "__main__":
    print("="*60)
    print("晋能控股招标数据 - 定时抓取服务")
    print(f"启动时间: {datetime.now()}")
    print("定时设置：每周三、周五 18:00 执行")
    
    # 检查Webhook URL配置
    webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
    if not webhook_url:
        print("⚠️  警告：未配置飞书机器人Webhook URL")
        print("请设置环境变量 FEISHU_WEBHOOK_URL")
        print("机器人通知功能将无法正常工作")
    
    # 检查其他必要的飞书配置
    required_envs = ['FEISHU_APP_ID', 'FEISHU_APP_SECRET', 'FEISHU_APP_TOKEN', 'FEISHU_TABLE_ID']
    missing_envs = [env for env in required_envs if not os.getenv(env)]
    if missing_envs:
        print(f"⚠️  警告：以下必要环境变量未设置: {missing_envs}")
        print("飞书多维表格上传功能将无法正常工作")
    
    print("="*60)

    # 创建调度器
    scheduler = BlockingScheduler()

    # 关键：添加Cron定时任务
    # 配置为每周三和周五的18:00执行
    scheduler.add_job(
        func=scheduled_crawler_job,   # 要执行的函数
        trigger=CronTrigger(
            day_of_week='wed,fri',    # 每周三、周五
            hour=18,                  # 18点
            minute=0,                 # 0分
            second=0                  # 0秒
        ),
        id='weekly_crawler',          # 任务ID
        name='每周三、五18点抓取晋能控股招标数据并同步至飞书',  # 任务名称
        replace_existing=True,        # 如果任务已存在则替换
        misfire_grace_time=3600       # 允许的容错时间（秒）
    )

    print("✅ 定时任务已添加：每周三、周五 18:00 执行完整流程。")
    print("⚠️  程序将持续在后台运行，等待执行定时任务...")
    print("⚠️  按 Ctrl+C 可以停止此服务。")
    print("-"*60)

    # 立即测试一次（可选，调试时开启）
    # print("\n🔧 立即执行一次测试任务...")
    # scheduled_crawler_job()

    try:
        # 启动调度器，程序会在这里阻塞，直到你手动停止
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n接收到停止信号，正在关闭调度器...")
        scheduler.shutdown()
        print("定时服务已停止。")