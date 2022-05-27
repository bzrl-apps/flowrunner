extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;
//use flowrunner::utils::*;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
//use tokio::runtime::Runtime;
use async_trait::async_trait;

use log::*;
//use tracing::*;

use evalexpr::*;

use std::path::Path;
use git2::{
    Repository,
    Cred,
    RemoteCallbacks,
    Oid,
    Signature,
    Commit,
    ObjectType,
    AnnotatedCommit,
    Reference
};

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct GitRepo {
    #[serde(default)]
    remote_url: String,
    local_dir: String,
    #[serde(default = "default_remote")]
    remote: String,
    #[serde(default = "default_branch")]
    branch: String,
    #[serde(default = "default_username")]
    username: String,
    #[serde(default = "default_usermail")]
    usermail: String,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    update: bool,
    auth: Option<Auth>,
    actions: Vec<Action>
}

fn default_remote() -> String {
    "origin".to_string()
}

fn default_branch() -> String {
    "master".to_string()
}

fn default_username() -> String {
    "flowrunner".to_string()
}

fn default_usermail() -> String {
    "flowrunner@bazarlab.io".to_string()
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Auth {
    mode: String,
    config: Map<String, Value>
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Action {
    name: String,
    #[serde(default)]
    cond: Option<String>,
    #[serde(default)]
    files: Vec<String>,
    #[serde(default)]
    commit_msg: String
}

#[async_trait]
impl Plugin for GitRepo {
    fn get_name(&self) -> String {
        env!("CARGO_PKG_NAME").to_string()
    }

    fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    fn get_description(&self) -> String {
        env!("CARGO_PKG_DESCRIPTION").to_string()
    }

    fn get_params(&self) -> Map<String, Value> {
        let params: Map<String, Value> = Map::new();

        params
    }

    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()> {
        let jops_params = JsonOps::new(Value::Object(params));
        let mut default = GitRepo::default();

        match jops_params.get_value_e::<String>("remote_url") {
            Ok(v) => default.remote_url = v,
            Err(_) => {},
        };

        match jops_params.get_value_e::<String>("local_dir") {
            Ok(v) => default.local_dir = v,
            Err(_) => { return Err(anyhow!("local can not be empty.")); },
        };

        match jops_params.get_value_e::<String>("remote") {
            Ok(v) => default.remote = v,
            Err(_) => { default.remote = "origin".to_string(); },
        };

        match jops_params.get_value_e::<String>("branch") {
            Ok(v) => default.branch = v,
            Err(_) => { default.branch = "master".to_string(); },
        };

        match jops_params.get_value_e::<String>("username") {
            Ok(v) => default.username = v,
            Err(_) => { default.username = "flowrunner".to_string(); },
        };

        match jops_params.get_value_e::<String>("usermail") {
            Ok(v) => default.usermail = v,
            Err(_) => { default.usermail = "flowrunner@bazarlab.io".to_string(); },
        };

        match jops_params.get_value_e::<bool>("recursive") {
            Ok(v) => default.recursive = v,
            Err(_) => {},
        };

        match jops_params.get_value_e::<bool>("update") {
            Ok(v) => default.update = v,
            Err(_) => {},
        };

        match jops_params.get_value_e::<Auth>("auth") {
            Ok(v) => {
                // Check if method is supported
                let methods = vec!["ssh", "userpass"];

                if !methods.contains(&v.mode.as_str()) {
                    return Err(anyhow!("Auth method must have one of the following values: {:?}", methods));
                }

                if v.config.is_empty() {
                    return Err(anyhow!("Auth config must be specified"));
                }

                if v.mode == "userpass" {
                    if !v.config.contains_key("password") {
                        return Err(anyhow!("Auth config must specify password for the userpass method"));
                    }
                } else {
                    if !v.config.contains_key("private_key") {
                        return Err(anyhow!("Auth config must have private_key for ssh method"));
                    }
                }

                default.auth = Some(v);
            },
            Err(_) => {},
        };

        match jops_params.get_value_e::<Vec<Action>>("actions") {
            Ok(v) => {
                let actions = vec!["add", "remove", "commit", "pull", "push", "fetch"];

                for a in v.iter() {
                    if !actions.contains(&a.name.as_str()) {
                        return Err(anyhow!("Action must be one of the following actions: {:?}", actions));
                    }

                    if (a.name == "add" || a.name == "delete") && a.files.is_empty() {
                        return Err(anyhow!("files must be specified when action is add"));
                    }

                    if a.name == "commit" && a.commit_msg.is_empty() {
                        return Err(anyhow!("commit_msg must be specified when action is commit"));
                    }
                }

                default.actions = v;
            },
            Err(_) => {},
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, _sender: Option<String>, _tx: &Vec<Sender<FlowMessage>>, _rx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        //let rt = Runtime::new().unwrap();
        //let _guard = rt.enter();

        // Initialize tracing
        //tracing_subscriber::fmt::init();

        // If remote_url is not empty  & local_dir doesnot exist => clone remote
        // If remote_url is empty => open with local_dir
        let local_dir_path = Path::new(self.local_dir.as_str());
        let repo = if !self.remote_url.is_empty() && !local_dir_path.exists() {
            // Create the local_dir
            if let Err(e) = std::fs::create_dir_all(local_dir_path) {
                return_plugin_exec_result_err!(result, e.to_string());
            }

            let callbacks = configure_remote_callbacks(self.username.as_str(), &self.auth);
            // Prepare fetch options.
            let mut fo = git2::FetchOptions::new();
            fo.remote_callbacks(callbacks);

            // Prepare builder.
            let mut builder = git2::build::RepoBuilder::new();
            builder.fetch_options(fo);
            builder.branch(self.branch.as_str());

            // Clone the project.
            info!("Cloning repository: remote_url={}, local_dir={}", self.remote_url, self.local_dir);
            match builder.clone(
                self.remote_url.as_str(),
                Path::new(self.local_dir.as_str()),
                ) {
                Ok(v) => v,
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            }
        } else {
            info!("Opening the local repository: local_dir={}", self.local_dir);
            match Repository::open(local_dir_path) {
                Ok(v) => {
                    info!("Pulling to update the local repository: local_dir={}", self.local_dir);
                    if self.update {
                        if let Err(e) = repo_pull(&v, &self) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    }

                    v
                },
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            }
        };


        for action in &self.actions {
            let cond = action.cond.as_ref()
                .and_then(|v| eval_boolean(v.as_str()).ok())
                .unwrap_or(true);

            if cond {
                match action.name.as_str() {
                    "fetch" => {
                        if let Err(e) = repo_fetch(&repo, &self) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    },
                    "pull" => {
                        if let Err(e) = repo_pull(&repo, &self) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    }
                    "add" => {
                        if let Err(e) = repo_add(&repo, action.files.clone()) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    },
                    "remove" => {
                        if let Err(e) = repo_remove(&repo, action.files.clone()) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    },
                    "commit" => {
                        if let Err(e) = repo_commit(
                                &repo,
                                &self,
                                action.commit_msg.as_str()
                            ) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    },
                    "push" => {
                        if let Err(e) = repo_push(&repo, &self) {
                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    },
                    _ => return_plugin_exec_result_err!(result, format!("action name {} is not supported", action.name)),
                }
            } else {
                warn!("Action is ignored: action={:?}, cond={:?}, eval={}",
                      action.name, action.cond, cond);
            }
        }

        result.status = Status::Ok;
        result
    }
}

fn configure_remote_callbacks<'a>(username: &'a str, auth_config: &'a Option<Auth>) -> RemoteCallbacks<'a> {
    // Prepare callbacks.
    let mut callbacks = RemoteCallbacks::new();

    if let Some(auth) = auth_config {
        if auth.mode == "ssh" {
            let private_key = auth.config.get("private_key")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            callbacks.credentials(|_url, _username_from_url, _allowed_types| {
                Cred::ssh_key(
                    username,
                    None,
                    Path::new(private_key),
                    None,
                )
            });
        } else {
            let password = auth.config.get("password")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            callbacks.credentials(|_url, _username_from_url, _allowed_types| {
                Cred::userpass_plaintext(username, password)
            });
        }
    }

    callbacks
}

fn repo_fetch(repo: &Repository, config: &GitRepo) -> Result<()> {
    // Prepare fetch options.
    let callbacks = configure_remote_callbacks(config.username.as_str(), &config.auth);
    let mut fo = git2::FetchOptions::new();
    fo.remote_callbacks(callbacks);

    // Always fetch all tags.
    // Perform a download and also update tips
    fo.download_tags(git2::AutotagOption::All);

    let mut remote = repo.find_remote(config.remote.as_str())?;

    remote.fetch(&[config.branch.as_str()], Some(&mut fo), None)?;

    Ok(())
}

fn repo_add(repo: &Repository, files: Vec<String>) -> Result<()> {
    let mut index = repo.index()?;

    let cb = &mut |path: &Path, _matched_spec: &[u8]| -> i32 {
        let status = repo.status_file(path).unwrap();

        let ret = if status.contains(git2::Status::WT_MODIFIED)
            || status.contains(git2::Status::WT_NEW)
            || status.contains(git2::Status::WT_DELETED)
            || status.contains(git2::Status::WT_TYPECHANGE)
            || status.contains(git2::Status::WT_RENAMED)
        {
            debug!("Adding file to commit: file={}", path.display());
            0
        } else {
            1
        };

        ret
    };

    index.add_all(files.iter(), git2::IndexAddOption::DEFAULT, Some(cb as &mut git2::IndexMatchedPath))?;
    index.write()?;

    Ok(())
}

fn repo_remove(repo: &Repository, files: Vec<String>) -> Result<()> {
    for f in files.iter() {
        let workdir = repo.workdir()
            .and_then(|p| Some(format!("{}", p.display())))
            .ok_or(anyhow!("Cannot get repository's workdir"))?;

        let file = format!("{}/{}", workdir, f);
        let p = Path::new(file.as_str());

        if p.is_dir() {
            std::fs::remove_dir_all(p)?;
        } else {
            std::fs::remove_file(p)?;
        }
    }

    repo_add(repo, files)?;

    Ok(())
}

fn repo_commit(repo: &Repository, config: &GitRepo, message: &str) -> Result<Oid> {
    let mut index = repo.index()?;
    let oid = index.write_tree()?;

    let signature = Signature::now(config.username.as_str(), config.usermail.as_str())?;
    let parent_commit = repo_get_last_commit(repo)?;
    let tree = repo.find_tree(oid)?;

    repo.commit(Some("HEAD"), //  point HEAD to our new commit
                &signature, // author
                &signature, // committer
                message, // commit message
                &tree, // tree
                &[&parent_commit])// parents
        .map_err(|e| anyhow!(e))
}

fn repo_get_last_commit(repo: &Repository) -> Result<Commit> {
    let obj = repo.head()?.resolve()?.peel(ObjectType::Commit)?;

    obj.into_commit().map_err(|_| anyhow!("Couldnot find commit"))
}

fn repo_push(repo: &Repository, config: &GitRepo) -> Result<()> {
    let mut remote = match repo.find_remote(config.remote.as_str()) {
        Ok(r) => r,
        Err(_) => repo.remote("origin", config.remote_url.as_str())?,
    };

    if config.auth.is_some() {
        let callbacks = configure_remote_callbacks(config.username.as_str(), &config.auth);
        remote.connect_auth(git2::Direction::Push, Some(callbacks), None)?;
    } else {
        remote.connect(git2::Direction::Push)?;
    }

    let refspec = format!(
        "refs/heads/{}:refs/heads/{}",
        config.branch,
        config.branch
    );

    // Prepare push options.
    let callbacks = configure_remote_callbacks(config.username.as_str(), &config.auth);
    let mut po = git2::PushOptions::new();
    po.remote_callbacks(callbacks);

    remote.push(&[refspec.as_str()], Some(&mut po))?;

    Ok(())
}

fn repo_merge(repo: &Repository, config: &GitRepo) -> Result<()> {
    let fetch_head = repo.find_reference("FETCH_HEAD")?;
    let fetch_commit = repo.reference_to_annotated_commit(&fetch_head)?;

    // 1. do a merge analysis
    let analysis = repo.merge_analysis(&[&fetch_commit])?;

    // 2. Do the appopriate merge
    if analysis.0.is_fast_forward() {
        info!("Doing a fast forward");
        // do a fast forward
        let refname = format!("refs/heads/{}", config.branch);

        match repo.find_reference(&refname) {
            Ok(mut git_ref) => {
                repo_do_fast_forward(repo, &mut git_ref, &fetch_commit)?;
            }
            Err(_) => {
                // The branch doesn't exist so just set the reference to the
                // commit directly. Usually this is because you are pulling
                // into an empty repository.
                repo.reference(
                    &refname,
                    fetch_commit.id(),
                    true,
                    &format!("Setting {} to {}", config.branch, fetch_commit.id()),
                )?;
                repo.set_head(&refname)?;
                repo.checkout_head(Some(
                    git2::build::CheckoutBuilder::default()
                        .allow_conflicts(true)
                        .conflict_style_merge(true)
                        .force(),
                ))?;
            }
        };
    } else if analysis.0.is_normal() {
        // do a normal merge
        let head_commit = repo.reference_to_annotated_commit(&repo.head()?)?;
        repo_do_normal_merge(&repo, config, &head_commit, &fetch_commit)?;
    } else {
        info!("Nothing to do...");
    }

    Ok(())
}

fn repo_do_fast_forward(
    repo: &Repository,
    git_ref: &mut Reference,
    fetch_commit: &AnnotatedCommit
) -> Result<()> {
    let name = match git_ref.name() {
        Some(s) => s.to_string(),
        None => String::from_utf8_lossy(git_ref.name_bytes()).to_string(),
    };

    let msg = format!("Fast-Forward: Setting {} to id: {}", name, fetch_commit.id());

    info!("{}", msg);

    git_ref.set_target(fetch_commit.id(), &msg)?;
    repo.set_head(&name)?;
    repo.checkout_head(Some(
        git2::build::CheckoutBuilder::default()
            // For some reason the force is required to make the working directory actually get updated
            // I suspect we should be adding some logic to handle dirty working directory states
            // but this is just an example so maybe not.
            .force(),
    ))?;

    Ok(())
}

fn repo_do_normal_merge(
    repo: &Repository,
    config: &GitRepo,
    head_commit: &git2::AnnotatedCommit,
    fetch_commit: &git2::AnnotatedCommit,
) -> Result<()> {
    let head_tree = repo.find_commit(head_commit.id())?.tree()?;
    let fetch_tree = repo.find_commit(fetch_commit.id())?.tree()?;

    let ancestor = repo
        .find_commit(repo.merge_base(head_commit.id(), fetch_commit.id())?)?
        .tree()?;

    let mut idx = repo.merge_trees(&ancestor, &head_tree, &fetch_tree, None)?;

    if idx.has_conflicts() {
        warn!("Merge conficts detected...");

        repo.checkout_index(Some(&mut idx), None)?;
        return Ok(());
    }

    let result_tree = repo.find_tree(idx.write_tree_to(repo)?)?;
    // now create the merge commit
    let msg = format!("Merge: {} into {}", fetch_commit.id(), head_commit.id());

    let signature = Signature::now(config.username.as_str(), config.usermail.as_str())?;
    let local_commit = repo.find_commit(head_commit.id())?;
    let remote_commit = repo.find_commit(fetch_commit.id())?;

    // Do our merge commit and set current branch head to that commit.
    let _merge_commit = repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        &msg,
        &result_tree,
        &[&local_commit, &remote_commit],
    )?;

    // Set working tree to match head.
    repo.checkout_head(None)?;

    Ok(())
}

fn repo_pull(repo: &Repository, config: &GitRepo) -> Result<()> {
    repo_fetch(repo, config)?;

    repo_merge(repo, config)?;

    Ok(())
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin GitRepo loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(GitRepo::default()))
}
