# feishu_notifier.py
import requests
import json
from datetime import datetime

class FeishuNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_text(self, text):
        """发送纯文本消息"""
        headers = {'Content-Type': 'application/json'}
        data = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        try:
            response = requests.post(self.webhook_url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            print(f"发送飞书消息失败: {e}")
            return None

    def send_crawler_report(self, total_count, success_count, duplicate_count, fail_count):
        """发送格式化的抓取报告"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 构造消息 - 去除本地文件信息，添加多维表格链接
        message = f"""🕷️ 招标数据抓取任务完成

🕒 执行时间：{current_time}
📊 抓取统计：
   • 发现新数据：{total_count} 条
   • 成功新增：{success_count} 条
   • 重复跳过：{duplicate_count} 条
   • 添加失败：{fail_count} 条

📋 查看最新数据：
https://ai.feishu.cn/base/OOYsbRScmaNEBYs5PsycX67anDb?table=tblZnQxACTwpTQN4&view=vewKAz70GX

（此消息由自动脚本发送）"""
        return self.send_text(message)

    def send_crawler_report_with_card(self, total_count, success_count, duplicate_count, fail_count):
        """使用卡片消息格式发送报告（更美观）"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 飞书卡片消息格式
        data = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": "📊 招标数据抓取报告"
                    },
                    "template": "blue"
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**执行时间**: {current_time}"
                        }
                    },
                    {
                        "tag": "div",
                        "fields": [
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**发现新数据**\n{total_count} 条"
                                }
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**成功新增**\n{success_count} 条"
                                }
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**重复跳过**\n{duplicate_count} 条"
                                }
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**添加失败**\n{fail_count} 条"
                                }
                            }
                        ]
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**查看最新数据**"
                        }
                    },
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {
                                    "tag": "plain_text",
                                    "content": "📋 打开多维表格"
                                },
                                "type": "primary",
                                "url": "https://ai.feishu.cn/base/OOYsbRScmaNEBYs5PsycX67anDb?table=tblZnQxACTwpTQN4&view=vewKAz70GX"
                            }
                        ]
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": "此消息由自动抓取脚本发送"
                            }
                        ]
                    }
                ]
            }
        }
        
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(self.webhook_url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            print(f"发送飞书卡片消息失败: {e}")
            # 失败时退回普通文本消息
            return self.send_crawler_report(total_count, success_count, duplicate_count, fail_count)

# 使用示例
if __name__ == "__main__":
    import os
    # 为避免在仓库中泄露Webhook示例密钥，请通过环境变量提供 FEISHU_WEBHOOK_URL
    WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")
    if not WEBHOOK_URL:
        print("未设置环境变量 FEISHU_WEBHOOK_URL，跳过示例调用。")
    else:
        notifier = FeishuNotifier(WEBHOOK_URL)
        
        # 测试文本消息
        notifier.send_text("测试：飞书机器人通知功能正常！")
        
        # 测试报告消息（文本格式）
        notifier.send_crawler_report(
            total_count=15,
            success_count=10,
            duplicate_count=3,
            fail_count=2
        )
        
        # 测试卡片消息（更美观）
        notifier.send_crawler_report_with_card(
            total_count=15,
            success_count=10,
            duplicate_count=3,
            fail_count=2
        )