<template>
  <section class="page" data-module="throttle">
    <header class="page-head">
      <div>
        <h2>采集频率控制管理</h2>
        <p class="page-desc">维护采集频率的请求间隔、日上限、并发与优先级权重；排队按轮次分配名额，额度耗尽时给出等待顺序与预计释放时间。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="runRounds">推进一轮调度</button>
        <button class="btn" type="button" @click="exportRows">导出采集频率控制清单</button>
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
          <th>排队情况</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="queue-cell">
            <template v-if="summaryOf(row)">
              <span class="badge">{{ summaryOf(row)!.used }} / {{ summaryOf(row)!.limit }}</span>
              <span class="muted">等待 {{ summaryOf(row)!.waiting }} · 运行 {{ summaryOf(row)!.running }}</span>
              <span v-if="summaryOf(row)!.depleted" class="badge warn">额度耗尽</span>
            </template>
            <span v-else class="muted">未启用排队</span>
          </td>
          <td class="row-actions">
            <button v-for="action in actions" :key="action" class="link" type="button" @click="runAction(action, row)">
              {{ action }}
            </button>
            <button class="link" type="button" @click="selectBoard(row)">查看队列</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无采集频率控制数据，可先登记频率策略</td>
        </tr>
      </tbody>
    </table>

    <section v-if="selected" class="queue-panel" data-role="queue-board">
      <header class="queue-head">
        <div>
          <h3>{{ selected.策略编号 }} · {{ selected.目标站点 }}</h3>
          <p class="page-desc">
            排队策略 {{ selected.排队策略 }} · 并发 {{ selected.并发数量 }} · 连续占用上限 {{ selected.连续占用上限 }}
            · 第 {{ selected.轮次 }} 轮已消费 {{ selected.额度.已消费额度 }} 次（剩余 {{ selected.额度.剩余额度 }}）
          </p>
        </div>
        <button class="btn ghost" type="button" @click="selected = null">收起</button>
      </header>

      <p v-if="selected.额度.耗尽" class="notice">
        日请求上限已耗尽，等待中的计划将在 {{ selected.额度.重置时间 }} 额度恢复后继续；高优先级也不会越过上限。
      </p>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">本轮已消费额度</span>
          <strong class="stat-value">{{ selected.本轮.已消费额度 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">运行中</span>
          <strong class="stat-value">{{ selected.运行中.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">等待中</span>
          <strong class="stat-value">{{ selected.等待中.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">被降级的计划</span>
          <strong class="stat-value">{{ selected.被降级的计划.length }}</strong>
        </article>
      </div>

      <table class="data-table">
        <caption class="table-caption">等待顺序（位置 1 为下一轮最先分配）</caption>
        <thead>
          <tr><th>位置</th><th>计划编号</th><th>优先级</th><th>有效优先级</th><th>等待轮数</th><th>等待原因</th><th>预计释放时间</th><th>降级</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in selected.等待中" :key="item.record_id">
            <td>{{ item.排队位置 }}</td>
            <td>{{ item.计划编号 }}</td>
            <td>{{ item.计划优先级 }}</td>
            <td>{{ item.有效优先级 }}</td>
            <td>{{ item.等待轮数 }}</td>
            <td>{{ item.等待原因 }}</td>
            <td>{{ item.预计释放时间 || '—' }}</td>
            <td>
              <span v-if="item.被降级" class="badge warn">{{ item.降级原因 }}</span>
              <span v-else>—</span>
            </td>
          </tr>
          <tr v-if="!selected.等待中.length">
            <td colspan="8" class="empty-state">当前没有等待中的计划</td>
          </tr>
        </tbody>
      </table>

      <table class="data-table">
        <caption class="table-caption">运行中 / 释放中</caption>
        <thead>
          <tr><th>计划编号</th><th>状态</th><th>轮次</th><th>开始时间</th><th>预计释放时间</th><th>下一次流转</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in selected.运行中" :key="item.record_id">
            <td>{{ item.计划编号 }}</td>
            <td>{{ item.状态 }}</td>
            <td>{{ item.轮次 }}</td>
            <td>{{ item.开始时间 || '—' }}</td>
            <td>{{ item.预计释放时间 || '—' }}</td>
            <td>{{ item.下一次流转 || '—' }}</td>
          </tr>
          <tr v-if="!selected.运行中.length">
            <td colspan="6" class="empty-state">当前没有运行中的请求</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集频率控制记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type QueueItem = {
  record_id: number
  计划编号: string | null
  计划优先级: string | null
  有效优先级: number
  排队位置: number
  等待轮数: number
  等待原因: string
  预计释放时间: string
  被降级: boolean
  降级原因: string
}
type Board = {
  strategy_id: number
  策略编号: string
  目标站点: string
  排队策略: string
  并发数量: number
  连续占用上限: number
  轮次: number
  本轮: { 已消费额度: number }
  额度: { 已消费额度: number; 剩余额度: number; 日请求上限: number; 耗尽: boolean; 重置时间: string }
  运行中: { record_id: number; 计划编号: string; 状态: string; 轮次: number; 开始时间: string; 预计释放时间: string; 下一次流转: string }[]
  等待中: QueueItem[]
  被降级的计划: QueueItem[]
}

const ENDPOINT = '/api/throttle'
const columns = ["策略编号", "目标站点", "请求间隔", "日请求上限", "并发数量", "排队策略", "优先级权重", "是否启用"]
const actions = ["提交校验", "启用策略", "停用策略"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const stats = ref([{"label": "频率策略总数", "value": 0}, {"label": "已启用策略", "value": 0}, {"label": "排队中计划", "value": 0}])

const rows = ref<Row[]>([])
const boards = ref<Record<string, Board>>({})
const selected = ref<Board | null>(null)
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function boardOf(row: Row): Board | undefined {
  return boards.value[String(row.id)]
}

function summaryOf(row: Row) {
  const board = boardOf(row)
  if (!board) {
    return null
  }
  return {
    used: board.额度.已消费额度,
    limit: board.额度.日请求上限,
    waiting: board.等待中.length,
    running: board.运行中.length,
    depleted: board.额度.耗尽,
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function selectBoard(row: Row) {
  selected.value = boardOf(row) ?? null
  if (!selected.value) {
    errorMessage.value = '该策略未启用，暂时没有排队看板'
  }
}

async function loadBoards() {
  const response = await request(`${ENDPOINT}/queue`)
  if (!response.ok) {
    throw new Error('排队看板读取失败')
  }
  const payload = await response.json()
  const next: Record<string, Board> = {}
  for (const item of payload.items ?? []) {
    next[String(item.strategy_id)] = item as Board
  }
  boards.value = next
  if (selected.value) {
    selected.value = next[String(selected.value.strategy_id)] ?? null
  }
}

async function runRounds() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/queue/rounds`, { method: 'POST', body: JSON.stringify({ values: {} }) })
    if (!response.ok) {
      throw new Error('轮次推进失败')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '轮次推进失败'
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
      throw new Error('采集频率控制动作未生效，请稍后重试')
    }
    const payload = await response.json()
    if (payload.ok === false) {
      throw new Error(payload.message ?? '采集频率控制动作未生效')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集频率控制操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('频率策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadBoards()
    stats.value[0].value = total.value
    stats.value[1].value = rows.value.filter((row) => row.status === '已启用').length
    stats.value[2].value = Object.values(boards.value).reduce((sum, board) => sum + (board.等待中?.length ?? 0), 0)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集频率控制列表读取失败'
  }
}

onMounted(reload)
</script>
