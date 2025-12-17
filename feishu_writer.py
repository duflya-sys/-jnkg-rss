import requests
import json
import pandas as pd
from datetime import datetime
import time
import os

class FeishuBitableWriter:
    def __init__(self, app_id, app_secret, app_token, table_id, debug=False):
        """
        初始化飞书多维表格写入器
        
        Args:
            app_id: 飞书应用的 App ID
            app_secret: 飞书应用的 App Secret
            app_token: 多维表格的 app_token (从URL获取)
            table_id: 多维表格的 table_id (从URL获取)
            debug: 是否启用调试模式
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        self.access_token = None
        self.token_expire_time = 0
        self.debug = debug
        
        # 检查必要的配置
        if not all([app_id, app_secret, app_token, table_id]):
            raise ValueError("飞书配置参数不全，请提供完整的app_id, app_secret, app_token, table_id")
        
        # 初始化时获取token
        self._get_access_token()
    
    def _get_access_token(self):
        """获取飞书开放平台接口调用凭证"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            result = response.json()
            
            if result.get("code") == 0:
                self.access_token = result["tenant_access_token"]
                self.token_expire_time = time.time() + result["expire"] - 300
                print(f"Access token 获取成功，有效期至: {datetime.fromtimestamp(self.token_expire_time)}")
            else:
                print(f"获取 access token 失败: {result}")
                self.access_token = None
        except Exception as e:
            print(f"获取 access token 异常: {e}")
            self.access_token = None
    
    def _check_token(self):
        """检查token是否有效，无效则重新获取"""
        if not self.access_token or time.time() >= self.token_expire_time:
            print("Access token 已过期或无效，重新获取...")
            self._get_access_token()
    
    def add_records(self, df, unique_key_field='项目编号'):
        """
        将DataFrame中的数据添加到飞书多维表格
        
        Args:
            df: 包含要添加数据的DataFrame
            unique_key_field: 用于去重的唯一标识字段名
        
        Returns:
            tuple: (成功数量, 失败数量, 重复数量)
        """
        if df.empty:
            print("没有数据需要添加")
            return 0, 0, 0
        
        self._check_token()
        if not self.access_token:
            print("无法获取有效的 access token，停止操作")
            return 0, 0, 0
        
        print("🔍 开始获取现有记录用于去重...")
        existing_records = self._get_existing_records()
        existing_keys = set(existing_records.keys()) if existing_records else set()
        
        print(f"当前表格已有 {len(existing_keys)} 条记录")
        
        # 准备要添加的新记录
        new_records = []
        duplicate_count = 0
        
        for idx, row in df.iterrows():
            # 构建唯一标识（使用标题+发布时间组合）
            record_title = str(row.get('项目名称', '')) if pd.notna(row.get('项目名称')) else ''
            if not record_title:
                record_title = str(row.get('标题', '')) if pd.notna(row.get('标题')) else ''
            
            publish_date = str(row.get('发布时间', '')) if pd.notna(row.get('发布时间')) else ''
            
            if record_title and publish_date:
                unique_key = f"{record_title}_{publish_date}"
            else:
                # 如果没有标题和日期，使用项目编号
                unique_key = str(row.get('项目编号', '')) if pd.notna(row.get('项目编号')) else ''
                if not unique_key:
                    continue  # 如果没有唯一标识，跳过
            
            # 去重检查
            if unique_key in existing_keys:
                duplicate_count += 1
                if self.debug and duplicate_count <= 3:
                    print(f"  跳过重复记录: {unique_key[:50]}...")
                continue
            
            record_data = self._build_record_fields(row)
            if record_data:
                new_records.append({"fields": record_data})
                existing_keys.add(unique_key)
        
        if not new_records:
            print(f"所有 {len(df)} 条记录都已存在，没有新数据需要添加")
            return 0, 0, duplicate_count
        
        print(f"准备添加 {len(new_records)} 条新记录，跳过 {duplicate_count} 条重复记录")
        
        # 分批添加记录
        success_count = 0
        fail_count = 0
        batch_size = 100
        
        for i in range(0, len(new_records), batch_size):
            batch = new_records[i:i+batch_size]
            batch_success, batch_fail = self._add_batch_records(batch)
            success_count += batch_success
            fail_count += batch_fail
            
            if i + batch_size < len(new_records):
                time.sleep(0.5)
        
        return success_count, fail_count, duplicate_count
    
    def _get_existing_records(self):
        """
        获取表格中现有的记录
        
        Returns:
            dict: {唯一标识: 记录ID} 的映射
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        existing_records = {}
        page_token = ""
        page_size = 100
        
        try:
            while True:
                params = {"page_size": page_size}
                if page_token:
                    params["page_token"] = page_token
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if self.debug:
                    print(f"  获取现有记录 - 状态码: {response.status_code}")
                
                result = response.json()
                
                if result.get("code") == 0:
                    data = result.get("data", {})
                    items = data.get("items", [])
                    
                    for item in items:
                        record_id = item.get("record_id")
                        fields = item.get("fields", {})
                        
                        # 构建唯一标识（与添加时一致）
                        title = fields.get("项目名称", "")
                        if not title:
                            title = fields.get("标题", "")
                        publish_date = fields.get("发布时间", "")
                        
                        if title and publish_date:
                            unique_key = f"{title}_{publish_date}"
                        else:
                            unique_key = fields.get("项目编号", "")
                        
                        if unique_key:
                            existing_records[str(unique_key)] = record_id
                        else:
                            # 如果没有唯一标识，使用record_id
                            existing_records[record_id] = record_id
                    
                    page_token = data.get("page_token", "")
                    if not page_token:
                        break
                else:
                    print(f"❌ 获取现有记录失败: {result.get('msg')}")
                    break
                    
        except Exception as e:
            print(f"获取现有记录异常: {e}")
        
        print(f"获取到 {len(existing_records)} 条现有记录")
        return existing_records
    
    def _format_date_for_feishu(self, date_str):
        """
        将字符串日期转换为飞书API所需的Unix时间戳（毫秒）
        """
        if not date_str or pd.isna(date_str):
            return None
        
        try:
            # 尝试解析常见的日期字符串格式
            for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日', '%Y.%m.%d'):
                try:
                    dt = datetime.strptime(str(date_str).strip(), fmt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue
            # 使用pandas的灵活解析
            dt = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(dt):
                return None
            return int(dt.timestamp() * 1000)
        except Exception as e:
            if self.debug:
                print(f"⚠️ 日期转换失败: {date_str}, 错误: {e}")
            return None
    
    def _build_record_fields(self, row):
        """将DataFrame行转换为飞书多维表格字段格式"""
        fields = {}
        
        # 更智能的字段映射
        # 优先使用"项目名称"，如果没有则用"标题"
        title_value = None
        if '项目名称' in row and pd.notna(row['项目名称']):
            title_value = str(row['项目名称'])
        elif '标题' in row and pd.notna(row['标题']):
            title_value = str(row['标题'])
        
        if title_value:
            fields['项目名称'] = title_value
        
        # 发布时间
        if '发布时间' in row and pd.notna(row['发布时间']):
            timestamp = self._format_date_for_feishu(str(row['发布时间']))
            if timestamp:
                fields['发布时间'] = timestamp
        
        # 采购单位
        if '采购单位' in row and pd.notna(row['采购单位']):
            fields['采购单位'] = str(row['采购单位'])
        
        # 项目编号
        if '项目编号' in row and pd.notna(row['项目编号']):
            fields['项目编号'] = str(row['项目编号'])
        
        # 链接（如果有链接字段）
        if '链接' in row and pd.notna(row['链接']):
            fields['链接'] = {
                "link": str(row['链接']),
                "text": "查看详情"
            }
        
        # 其他可能需要的字段
        if '采购方式' in row and pd.notna(row['采购方式']):
            fields['采购方式'] = str(row['采购方式'])
        
        if '省份' in row and pd.notna(row['省份']):
            fields['省份'] = str(row['省份'])
        
        if '城市' in row and pd.notna(row['城市']):
            fields['城市'] = str(row['城市'])
        
        if self.debug and fields:
            print(f"生成的字段: {list(fields.keys())}")
        
        return fields
    
    def _add_batch_records(self, records):
        """批量添加记录到飞书多维表格"""
        if not records:
            return 0, 0
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "records": records
        }
        
        if self.debug:
            print(f"📤 正在批量添加 {len(records)} 条记录...")
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            
            result = response.json()
            
            if result.get("code") == 0:
                success_count = len(result.get("data", {}).get("records", []))
                print(f"✅ 成功添加 {success_count} 条记录")
                return success_count, 0
            else:
                print(f"❌ 添加记录失败: {result.get('msg')}")
                return 0, len(records)
                
        except Exception as e:
            print(f"添加记录异常: {e}")
            return 0, len(records)
    
    def list_table_fields(self):
        """列出表格的所有字段（列名）"""
        self._check_token()
        if not self.access_token:
            print("无法获取有效的 access token")
            return None
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            result = response.json()
            
            if result.get("code") == 0:
                fields = result.get("data", {}).get("items", [])
                print("\n📋 飞书表格字段列表:")
                for i, field in enumerate(fields, 1):
                    field_name = field.get("field_name")
                    field_type = field.get("type")
                    print(f"  {i}. {field_name} ({field_type})")
                return fields
            else:
                print(f"❌ 获取字段列表失败: {result}")
                return None
        except Exception as e:
            print(f"获取字段列表异常: {e}")
            return None
    
    def get_all_records(self):
        """获取表格中的所有记录（用于调试）"""
        self._check_token()
        if not self.access_token:
            print("无法获取有效的 access token")
            return None
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        all_records = []
        page_token = ""
        page_size = 500
        
        try:
            while True:
                params = {"page_size": page_size}
                if page_token:
                    params["page_token"] = page_token
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                result = response.json()
                
                if result.get("code") == 0:
                    data = result.get("data", {})
                    items = data.get("items", [])
                    all_records.extend(items)
                    
                    page_token = data.get("page_token", "")
                    if not page_token:
                        break
                else:
                    print(f"获取记录失败: {result}")
                    break
                    
        except Exception as e:
            print(f"获取记录异常: {e}")
        
        print(f"总共获取到 {len(all_records)} 条记录")
        
        # 显示前5条记录的内容
        print("\n📄 前5条记录内容:")
        for i, record in enumerate(all_records[:5], 1):
            print(f"\n记录 {i} (ID: {record.get('record_id')}):")
            fields = record.get("fields", {})
            for key, value in fields.items():
                print(f"  {key}: {value}")
        
        return all_records


# 测试函数
def test_full_process():
    """完整的测试流程"""
    print("🧪 开始测试飞书多维表格完整流程...")
    
    # 从环境变量读取配置
    app_id = os.getenv('FEISHU_APP_ID', '')
    app_secret = os.getenv('FEISHU_APP_SECRET', '')
    app_token = os.getenv('FEISHU_APP_TOKEN', '')
    table_id = os.getenv('FEISHU_TABLE_ID', '')
    
    if not all([app_id, app_secret, app_token, table_id]):
        print("❌ 飞书配置不完整，请设置以下环境变量:")
        print("   FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_TOKEN, FEISHU_TABLE_ID")
        return
    
    writer = FeishuBitableWriter(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        table_id=table_id,
        debug=True
    )
    
    # 1. 查看表格字段
    writer.list_table_fields()
    
    # 2. 查看现有记录
    print("\n🔍 查看现有记录...")
    writer.get_all_records()
    
    # 3. 创建测试数据
    print("\n📝 创建测试数据...")
    test_data = pd.DataFrame([{
        '项目名称': '晋能控股测试项目',
        '标题': '东大矿井瓦斯实验室工程',
        '发布时间': datetime.now().strftime('%Y-%m-%d'),
        '采购单位': '晋圣公司',
        '项目编号': f"TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        '链接': 'https://dzzb.jnkgjtdzzbgs.com/1ywgg1/20251218/1185901592180686849.html',
        '采购方式': '公开招标',
        '省份': '山西省',
        '城市': '太原市'
    }])
    
    # 4. 添加记录
    print("\n📤 尝试添加测试记录...")
    success, fail, duplicate = writer.add_records(test_data, unique_key_field='项目编号')
    print(f"\n📊 测试结果: 成功={success}, 失败={fail}, 重复={duplicate}")


if __name__ == "__main__":
    test_full_process()