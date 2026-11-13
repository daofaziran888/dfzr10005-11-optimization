<template>
  <section class="page" data-module="baseline">
    <header class="page-head">
      <div>
        <h2>基线监控管理</h2>
        <p class="page-desc">关键路径、承诺/预警窗口与风险等级统一取自路径计算中心的不可变摘要快照；刷新或返回列表保持同一份路径版本。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记基线配置</button>
        <button class="btn" type="button" @click="exportRows">导出基线监控管理清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>基线编号</span>
        <input v-model="keyword" placeholder="按基线编号检索" />
      </label>
      <label class="filter-item">
        <span>监控状态</span>
        <input v-model="statusFilter" placeholder="草稿/监控中/已延期/已关闭" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">
            <span v-if="column === '风险等级'" :class="['risk-badge', riskClass(row[column])]">{{ row[column] ?? '—' }}</span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="showVersions(row)">版本追溯</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无基线监控数据，可先登记基线配置</td>
        </tr>
      </tbody>
    </table>

    <section v-if="versionTrace.length" class="trace-panel">
      <header class="trace-head">
        <strong>{{ traceTitle }} · 路径版本与承诺窗口对应关系</strong>
        <button class="btn ghost" type="button" @click="versionTrace = []">关闭</button>
      </header>
      <table class="data-table">
        <thead>
          <tr><th>路径版本</th><th>口径版本</th><th>计算水位</th><th>承诺时间</th><th>预警时间</th><th>超时预算(分钟)</th><th>来源批次</th><th>计算时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="version in versionTrace" :key="version.path_version">
            <td>{{ version.path_version }}</td>
            <td>v{{ version.caliber_version }}</td>
            <td>{{ version.watermark }}</td>
            <td>{{ version.promise_time ?? '—' }}</td>
            <td>{{ version.warn_time ?? '—' }}</td>
            <td>{{ version.timeout_budget_minutes }}</td>
            <td>{{ version.batch_id }}</td>
            <td>{{ version.computed_at }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条基线监控记录 · 数据与 SLA 风险同步一致</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

interface BaselineVersion {
  path_version: string
  caliber_version: number
  watermark: number
  promise_time: string | null
  warn_time: string | null
  timeout_budget_minutes: number
  batch_id: string
  computed_at: string
}

const ENDPOINT = '/api/baseline'
const columns = ['基线编号', '基线名称', '关键路径', 'path_version', 'watermark', '承诺时间', '预警时间', '最近产出', '超时预算分钟', '上一版超时预算分钟', '风险等级', '承诺窗口对应版本']
const actions = ['启用监控', '调整基线', '关闭基线']

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const keyword = ref('')
const statusFilter = ref('')
const versionTrace = ref<BaselineVersion[]>([])
const traceTitle = ref('')

const stats = computed(() => [
  { label: '基线配置总数', value: rows.value.length },
  { label: '监控中基线', value: rows.value.filter((row) => row.status === '监控中').length },
  { label: '高风险基线', value: rows.value.filter((row) => row['风险等级'] === '高风险').length },
])

function riskClass(level: unknown): string {
  if (level === '高风险') return 'risk-high'
  if (level === '中风险') return 'risk-mid'
  if (level === '已完成') return 'risk-done'
  return 'risk-low'
}

function resetFilters() {
  keyword.value = ''
  statusFilter.value = ''
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '基线配置登记入口尚未接入审批流'
}

async function showVersions(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/versions`)
    if (!response.ok) {
      throw new Error('历史基线版本读取失败')
    }
    const payload = await response.json()
    versionTrace.value = payload.versions ?? []
    traceTitle.value = String(row['基线编号'])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史基线版本读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('基线监控管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '基线监控管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams()
  if (keyword.value) query.set('keyword', keyword.value)
  if (statusFilter.value) query.set('status', statusFilter.value)
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) {
      throw new Error('基线配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '基线监控管理列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.risk-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
.risk-high { background: #fee4e2; color: #b42318; }
.risk-mid { background: #fef0c7; color: #b54708; }
.risk-low { background: #dcfae6; color: #067647; }
.risk-done { background: #e0eaff; color: #1849a9; }
.trace-panel { margin-top: 16px; border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: #fff; }
.trace-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
</style>
