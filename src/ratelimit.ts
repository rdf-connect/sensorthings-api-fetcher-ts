// ratelimit.ts
type QueuedRequest = {
    url: string;
    delay: number;
    resolve: (value: Response) => void;
    reject: (reason?: unknown) => void;
};

const TRANSIENT_HTTP_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);
const TRANSIENT_NETWORK_CODES = new Set([
    "ECONNRESET",
    "ETIMEDOUT",
    "EAI_AGAIN",
    "ENOTFOUND",
    "ECONNREFUSED",
    "EPIPE",
]);

const requestQueue: QueuedRequest[] = [];
let isProcessing = false;

/**
 * Rate limit all fetch requests on a 250 ms delay minimum
 */
export function rateLimitedFetch(
    url: string,
    delay: number = 250,
): Promise<Response> {
    return new Promise((resolve, reject) => {
        requestQueue.push({ url, delay, resolve, reject });
        processQueue();
    });
}

async function processQueue() {
    if (isProcessing || requestQueue.length === 0) {
        return;
    }

    isProcessing = true;

    const { url, delay, resolve, reject } = requestQueue.shift()!;

    try {
        const response = await fetchWithRetry(url);
        resolve(response);
    } catch (err: unknown) {
        reject(err);
    }

    // Use the request’s own delay before moving on
    setTimeout(() => {
        isProcessing = false;
        processQueue();
    }, delay);
}

async function fetchWithRetry(
    url: string,
    maxAttempts: number = 5,
    initialBackoffMs: number = 500,
): Promise<Response> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            const response = await fetch(url);

            if (
                !response.ok &&
                TRANSIENT_HTTP_STATUS.has(response.status) &&
                attempt < maxAttempts
            ) {
                const waitMs = initialBackoffMs * 2 ** (attempt - 1);
                await sleep(waitMs);
                continue;
            }

            return response;
        } catch (err: unknown) {
            lastError = err;

            if (!isTransientNetworkError(err) || attempt >= maxAttempts) {
                throw err;
            }

            const waitMs = initialBackoffMs * 2 ** (attempt - 1);
            await sleep(waitMs);
        }
    }

    throw lastError ?? new Error(`Fetch failed for ${url}`);
}

function isTransientNetworkError(err: unknown): boolean {
    if (!(err instanceof Error)) {
        return false;
    }

    const anyErr = err as Error & {
        code?: string;
        cause?: { code?: string };
    };

    const code = anyErr.code ?? anyErr.cause?.code;
    return !!code && TRANSIENT_NETWORK_CODES.has(code);
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
