<template>
  <section class="page" data-module="dag">
    <header class="page-head">
      <div>
        <h2>任务依赖编排管理</h2>
        <p class="page-desc">维护任务依赖DAG的节点关系、执行条件与失败策略，为任务依赖编排提供可审计的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记依赖图节点</button>
        <button class="btn" type="button" @click="exportRows">导出任务依赖编排清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
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
            <span v-if="column === '节点状态'" :class="['status-badge', statusClass(row[column])]">{{ row[column] ?? '—' }}</span>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无任务依赖编排数据，可先登记依赖图节点</td>
        </tr>
      </tbody>
    </table>

    <section class="batch-panel">
      <header class="batch-head">
        <strong>状态事件批次 · 增量重算</strong>
        <span class="batch-hint">同批次多节点以同一快照计算优先级；仅重算变更节点及其下游路径</span>
      </header>
      <div class="batch-form">
        <label class="filter-item">
          <span>节点A</span>
          <select v-model="batchForm.nodeA">
            <option v-for="row in rows" :key="String(row.id)" :value="String(row['节点编号'])">{{ row['节点编号'] }}（{{ row['节点状态'] }}）</option>
          </select>
        </label>
        <label class="filter-item">
          <span>目标状态A</span>
          <select v-model="batchForm.statusA">
            <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>节点B（可空）</span>
          <select v-model="batchForm.nodeB">
            <option value="">不同时变更</option>
            <option v-for="row in rows" :key="String(row.id)" :value="String(row['节点编号'])">{{ row['节点编号'] }}（{{ row['节点状态'] }}）</option>
          </select>
        </label>
        <label class="filter-item">
          <span>目标状态B</span>
          <select v-model="batchForm.statusB">
            <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>幂等键</span>
          <input v-model="batchForm.idempotencyKey" placeholder="重复提交不重复缩短承诺" />
        </label>
        <label class="filter-item">
          <span>失败注入节点（可空）</span>
          <input v-model="batchForm.failNode" placeholder="如 DAG-0002，演示断点续算" />
        </label>
        <button class="btn primary" type="button" @click="submitBatch">提交批次</button>
        <button class="btn" type="button" :disabled="!failedBatchId" @click="resumeBatch">从断点续算</button>
      </div>
      <p v-if="batchResult" class="batch-result">
        批次 {{ batchResult.batch_id }} · {{ batchResult.status }} · 水位 {{ batchResult.watermark }}
        · 受影响 {{ batchResult.affected_paths.length }} 条 · 复用 {{ batchResult.reused_paths.length }} 条
        <span v-if="batchResult.error" class="error-text"> · {{ batchResult.error }}</span>
      </p>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务依赖编排记录 · 动作自动驱动路径增量重算</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
interface BatchResult {
  batch_id: string
  status: string
  watermark: number
  affected_paths: string[]
  reused_paths: string[]
  error: string
}

const ENDPOINT = '/api/dag'
const columns = ['节点编号', '节点名称', '上游节点', '下游节点', '超时分钟', '节点状态', '最近产出', '失败策略']
const actions = ['启动节点', '暂停节点', '完成节点']
const statuses = ['草稿', '运行中', '已暂停', '已完成']

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ['节点编号', '节点名称', '上游节点']
const batchResult = ref<BatchResult | null>(null)
const failedBatchId = ref('')
const batchForm = reactive({
  nodeA: 'DAG-0002',
  statusA: '已暂停',
  nodeB: '',
  statusB: '运行中',
  idempotencyKey: '',
  failNode: '',
})

const stats = computed(() => [
  { label: '依赖图节点总数', value: rows.value.length },
  { label: '运行中节点', value: rows.value.filter((row) => row['节点状态'] === '运行中').length },
  { label: '已完成节点', value: rows.value.filter((row) => row['节点状态'] === '已完成').length },
])

function statusClass(status: unknown): string {
  if (status === '已完成') return 'status-done'
  if (status === '运行中') return 'status-running'
  if (status === '已暂停') return 'status-paused'
  return 'status-draft'
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function submitBatch() {
  errorMessage.value = ''
  const events = [{ node: batchForm.nodeA, status: batchForm.statusA }]
  if (batchForm.nodeB) {
    events.push({ node: batchForm.nodeB, status: batchForm.statusB })
  }
  const values: Record<string, unknown> = { events }
  if (batchForm.idempotencyKey) values.idempotency_key = batchForm.idempotencyKey
  if (batchForm.failNode) values.fail_injection = { scope: 'node', target: batchForm.failNode }
  try {
    const response = await request(`${ENDPOINT}/events/batch`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}))
      throw new Error(payload.detail ?? '批次提交失败')
    }
    const result: BatchResult = await response.json()
    batchResult.value = result
    failedBatchId.value = result.status === 'failed' ? result.batch_id : ''
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次提交失败'
  }
}

async function resumeBatch() {
  if (!failedBatchId.value) return
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batches/${failedBatchId.value}/resume`, {
      method: 'POST',
      body: JSON.stringify({}),
    })
    if (!response.ok) {
      throw new Error('断点续算失败')
    }
    batchResult.value = await response.json()
    failedBatchId.value = ''
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '断点续算失败'
  }
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '依赖图节点登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('任务依赖编排动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务依赖编排操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('依赖图节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务依赖编排列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.status-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
.status-done { background: #dcfae6; color: #067647; }
.status-running { background: #e0eaff; color: #1849a9; }
.status-paused { background: #fef0c7; color: #b54708; }
.status-draft { background: #f2f4f7; color: #475467; }
.batch-panel { margin-top: 16px; border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: #fff; }
.batch-head { display: flex; gap: 12px; align-items: baseline; margin-bottom: 8px; }
.batch-hint { color: var(--muted); font-size: 12px; }
.batch-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
.batch-form select, .batch-form input { min-width: 150px; padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; }
.batch-result { margin-top: 10px; font-size: 13px; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
</style>
