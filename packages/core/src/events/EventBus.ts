import { EventEmitter } from "node:events";

export class EventBus {
  private readonly emitter = new EventEmitter();

  emit<T>(event: string, payload: T): void {
    this.emitter.emit(event, payload);
  }

  on<T>(
    event: string,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.emitter.on(event, listener);
  }

  once<T>(
    event: string,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.emitter.once(event, listener);
  }

  off<T>(
    event: string,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.emitter.off(event, listener);
  }

  removeAllListeners(event?: string): void {
    this.emitter.removeAllListeners(event);
  }

  listenerCount(event: string): number {
    return this.emitter.listenerCount(event);
  }
}