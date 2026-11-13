<template>
  <section class="page" data-module="taskplan">
    <header class="page-head">
      <div>
        <h2>采集任务编排管理</h2>
        <p class="page-desc">维护采集计划的站点配置、频率设置与优先级；提交请求后按频率策略的额度与并发排队，待等待清单显示当前排队位置。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="submitSelected">提交排队请求</button>
        <button class="btn" type="button" @click="exportRows">导出采集任务编排清单</button>
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
          <th>排队状态</th>
          <th>排队位置</th>
          <th>计划待办</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :class="{ 'row-selected': selectedIds.includes(Number(row.id)) }">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td><span class="badge">{{ row['排队状态'] ?? '未排队' }}</span></td>
          <td>{{ row['排队位置'] || '—' }}</td>
          <td>{{ row['待办'] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="toggleSelect(row)">选择</button>
            <button class="link" type="button" @click="submitOne(row)">提交请求</button>
            <button v-for="action in actions" :key="action" class="link" type="button" @click="runAction(action, row)">
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 4" class="empty-state">暂无采集任务编排数据，可先登记采集计划</td>
        </tr>
      </tbody>
    </table>

    <section class="queue-panel" data-role="waiting-list">
      <header class="queue-head">
        <div>
          <h3>待等待清单</h3>
          <p class="page-desc">等待中的计划按排队位置排序；位置 1 是下一轮最先分配名额的计划，预计释放时间来自当前占用与额度上限。</p>
        </div>
        <span class="muted">已选 {{ selectedIds.length }} 个计划</span>
      </header>
      <table class="data-table">
        <thead>
          <tr><th>排队位置</th><th>计划编号</th><th>策略编号</th><th>优先级</th><th>有效优先级</th><th>等待轮数</th><th>等待原因</th><th>预计释放时间</th><th>提醒</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in waiting" :key="String(item['排队记录'])">
            <td>{{ item['排队位置'] }}</td>
            <td>{{ item['计划编号'] }}</td>
            <td>{{ item['策略编号'] }}</td>
            <td>{{ item['计划优先级'] }}</td>
            <td>{{ item['有效优先级'] }}</td>
            <td>{{ item['等待轮数'] }}</td>
            <td>{{ item['等待原因'] }}</td>
            <td>{{ item['预计释放时间'] || '—' }}</td>
            <td><span v-if="item['超时提醒']" class="badge warn">已提醒</span><span v-else>—</span></td>
          </tr>
          <tr v-if="!waiting.length">
            <td colspan="9" class="empty-state">当前没有等待中的计划，提交请求后会出现在这里</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集任务编排记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type WaitingItem = Record<string, string | number | boolean | null>

const ENDPOINT = '/api/taskplan'
const columns = ["计划编号", "目标站点", "采集频率", "数据范围", "优先级", "资源配额", "预计用时", "计划状态"]
const actions = ["启动计划", "暂停计划", "恢复计划", "归档计划"]
const statuses = ["草稿", "运行中", "已暂停", "已完成"]
const stats = ref([{"label": "采集计划总数", "value": 0}, {"label": "运行中计划", "value": 0}, {"label": "等待中计划", "value": 0}])

const rows = ref<Row[]>([])
const waiting = ref<WaitingItem[]>([])
const selectedIds = ref<number[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function toggleSelect(row: Row) {
  const id = Number(row.id)
  selectedIds.value = selectedIds.value.includes(id)
    ? selectedIds.value.filter((item) => item !== id)
    : [...selectedIds.value, id]
}

async function submitOne(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/queue/submissions`, {
      method: 'POST',
      body: JSON.stringify({ values: {} }),
    })
    const payload = await response.json()
    if (!response.ok) {
      throw new Error(payload.detail ?? '提交排队请求失败')
    }
    if (payload.ok === false) {
      throw new Error(payload.message ?? '提交排队请求失败')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '提交排队请求失败'
  }
}

async function submitSelected() {
  if (!selectedIds.value.length) {
    errorMessage.value = '请先选择要提交的采集计划'
    return
  }
  for (const id of selectedIds.value) {
    const row = rows.value.find((item) => Number(item.id) === id)
    if (row) {
      await submitOne(row)
    }
  }
  selectedIds.value = []
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集任务编排动作未生效，请稍后重试')
    }
    const payload = await response.json()
    if (payload.ok === false) {
      throw new Error(payload.message ?? '采集任务编排动作未生效')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务编排操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('采集计划列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    const waitingResponse = await request(`${ENDPOINT}/queue/waiting`)
    const waitingPayload = await waitingResponse.json()
    waiting.value = waitingPayload.items ?? []
    stats.value[0].value = total.value
    stats.value[1].value = rows.value.filter((row) => row.status === '运行中').length
    stats.value[2].value = waiting.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务编排列表读取失败'
  }
}

onMounted(reload)
</script>
