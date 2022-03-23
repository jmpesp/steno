use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde::Serialize;
use slog::Drain;
use std::sync::{Arc, Mutex};
use steno::{
    ActionContext, ActionError, ActionFunc, SagaId, SagaTemplate,
    SagaTemplateBuilder, SagaType,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct CountingSaga;
impl SagaType for CountingSaga {
    type SagaParamsType = Arc<CountingSagaParams>;
    type ExecContextType = CountingSagaContext;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CountingSagaParams {
    start_at: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CountingSagaContext {
    number: Arc<Mutex<u8>>,
}

fn make_saga_id() -> SagaId {
    SagaId(Uuid::new_v4())
}

fn make_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn make_sec(log: &slog::Logger) -> steno::SecClient {
    steno::sec(log.new(slog::o!()), Arc::new(steno::InMemorySecStore::new()))
}

async fn init(sagactx: ActionContext<CountingSaga>) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let mut number = osagactx.number.lock().unwrap();
    *number = sagactx.saga_params().start_at;

    Ok(())
}

async fn add(sagactx: ActionContext<CountingSaga>) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let mut number = osagactx.number.lock().unwrap();
    *number += 1;

    Ok(())
}

async fn add_with_fail(
    sagactx: ActionContext<CountingSaga>,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let mut number = osagactx.number.lock().unwrap();
    *number += 1;

    Err(ActionError::InjectedError)
}

async fn subtract(sagactx: ActionContext<CountingSaga>) -> Result<()> {
    let osagactx = sagactx.user_data();
    let mut number = osagactx.number.lock().unwrap();
    *number -= 1;

    Ok(())
}

async fn end(_sagactx: ActionContext<CountingSaga>) -> Result<(), ActionError> {
    Ok(())
}

async fn fail(
    _sagactx: ActionContext<CountingSaga>,
) -> Result<(), ActionError> {
    Err(ActionError::InjectedError)
}

pub struct CountingSagaBuilder {
    pub node_names: Vec<String>,
    pub template: Arc<SagaTemplate<CountingSaga>>,
}

impl CountingSagaBuilder {
    pub async fn serial(
        times: u8,
        should_fail: bool,
    ) -> anyhow::Result<CountingSagaBuilder> {
        assert!(times > 0);

        let node_names: Vec<String> =
            (0..times).map(|i| format!("add{}", i)).collect();

        let mut w = SagaTemplateBuilder::new();

        w.append("init", "init", steno::new_action_noop_undo(init));

        for i in 0..times {
            w.append(
                node_names[i as usize].as_str(),
                node_names[i as usize].as_str(),
                ActionFunc::new_action(add, subtract),
            );
        }

        if should_fail {
            w.append("fail", "fail", steno::new_action_noop_undo(fail));
        } else {
            w.append("end", "end", steno::new_action_noop_undo(end));
        }

        Ok(CountingSagaBuilder { node_names, template: Arc::new(w.build()) })
    }

    pub async fn serial_fail_on_add(
        times: u8,
    ) -> anyhow::Result<CountingSagaBuilder> {
        assert!(times > 0);

        let node_names: Vec<String> =
            (0..times).map(|i| format!("add{}", i)).collect();

        let mut w = SagaTemplateBuilder::new();

        w.append("init", "init", steno::new_action_noop_undo(init));

        for i in 0..times {
            if i == (times - 1) {
                w.append(
                    node_names[i as usize].as_str(),
                    node_names[i as usize].as_str(),
                    ActionFunc::new_action(add_with_fail, subtract),
                );
            } else {
                w.append(
                    node_names[i as usize].as_str(),
                    node_names[i as usize].as_str(),
                    ActionFunc::new_action(add, subtract),
                );
            }
        }

        w.append("end", "end", steno::new_action_noop_undo(end));

        Ok(CountingSagaBuilder { node_names, template: Arc::new(w.build()) })
    }

    pub async fn parallel(
        times: u8,
        should_fail: bool,
    ) -> anyhow::Result<CountingSagaBuilder> {
        assert!(times > 0);

        let node_names: Vec<String> =
            (0..times).map(|i| format!("add{}", i)).collect();

        let mut w = SagaTemplateBuilder::new();

        w.append("init", "init", steno::new_action_noop_undo(init));

        w.append_parallel(
            (0..times)
                .map(|i| {
                    (
                        node_names[i as usize].as_str(),
                        node_names[i as usize].as_str(),
                        ActionFunc::new_action(add, subtract),
                    )
                })
                .collect(),
        );

        if should_fail {
            w.append("fail", "fail", steno::new_action_noop_undo(fail));
        } else {
            w.append("end", "end", steno::new_action_noop_undo(end));
        }

        Ok(CountingSagaBuilder { node_names, template: Arc::new(w.build()) })
    }
}

pub async fn make_counting_saga(
    template: Arc<SagaTemplate<CountingSaga>>,
    start_at: u8,
) -> anyhow::Result<Arc<CountingSagaContext>> {
    let saga_context =
        Arc::new(CountingSagaContext { number: Arc::new(Mutex::new(0)) });

    let saga_params = Arc::new(CountingSagaParams { start_at });

    let saga_id = make_saga_id();
    let log = make_log();
    let sec = make_sec(&log);
    let future = sec
        .saga_create(
            saga_id,
            saga_context.clone(),
            template,
            "counting saga".to_string(),
            saga_params.clone(),
        )
        .await
        .expect("failed to create saga");

    sec.saga_start(saga_id).await.expect("failed to start saga");

    let _result = future.await;

    let _saga = sec
        .saga_get(saga_id)
        .await
        .map_err(|_: ()| anyhow!("failed to fetch saga after running it"))?;

    Ok(saga_context)
}

/// Ensure that basic counting saga (serial nodes that perform add actions) counts to N
#[tokio::test]
pub async fn serial_count_from_0_to_n() -> anyhow::Result<()> {
    for i in 1..100 {
        let start_at = 0;
        let times = i;

        let builder = CountingSagaBuilder::serial(times, false).await?;

        let saga_context =
            make_counting_saga(builder.template, start_at).await?;

        assert_eq!(*saga_context.number.lock().unwrap(), i);
    }

    Ok(())
}

/// Ensure that basic counting saga (parallel nodes that perform add actions) counts to N
#[tokio::test]
pub async fn parallel_count_from_0_to_n() -> anyhow::Result<()> {
    for i in 1..100 {
        let start_at = 0;
        let times = i;

        let builder = CountingSagaBuilder::parallel(times, false).await?;

        let saga_context =
            make_counting_saga(builder.template, start_at).await?;

        assert_eq!(*saga_context.number.lock().unwrap(), i);
    }

    Ok(())
}

/// Ensure that basic serial counting saga where the "end" node fails and as a
/// result leaves the context number at 0.
#[tokio::test]
pub async fn serial_count_from_0_to_n_with_fail() -> anyhow::Result<()> {
    for i in 1..100 {
        let start_at = 0;
        let times = i;

        let builder = CountingSagaBuilder::serial(times, true).await?;

        let saga_context =
            make_counting_saga(builder.template, start_at).await?;

        assert_eq!(*saga_context.number.lock().unwrap(), 0);
    }

    Ok(())
}

/// Ensure that basic parallel counting saga where the "end" node fails and as a
/// result leaves the context number at 0.
#[tokio::test]
pub async fn parallel_count_from_0_to_n_with_fail() -> anyhow::Result<()> {
    for i in 1..100 {
        let start_at = 0;
        let times = i;

        let builder = CountingSagaBuilder::parallel(times, true).await?;

        let saga_context =
            make_counting_saga(builder.template, start_at).await?;

        assert_eq!(*saga_context.number.lock().unwrap(), 0);
    }

    Ok(())
}

/// Ensure that basic serial counting saga where the last "add" node fails
/// unwinds and as a result leaves the context number at 0.
#[tokio::test]
pub async fn serial_count_with_fail_on_add() -> anyhow::Result<()> {
    let start_at = 0;
    let times = 10;

    let builder = CountingSagaBuilder::serial_fail_on_add(times).await?;

    let saga_context =
        make_counting_saga(builder.template, start_at).await?;

    assert_eq!(*saga_context.number.lock().unwrap(), 0);

    Ok(())
}
